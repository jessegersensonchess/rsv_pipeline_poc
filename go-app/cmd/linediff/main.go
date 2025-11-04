// Package main provides a high-throughput diff helper that prints lines from
// file2 that are not present in file1. It is optimized for very large inputs,
// minimal allocations, and good cache behavior.
//
// Key techniques:
//   - A compact two-level index over 48-bit hashes of file1 lines:
//     top 16 bits select a bin; within each bin, low 32-bit tails are sorted.
//   - Parallel scanning of file2 via disjoint byte ranges, aligned to newlines.
//   - Type-specialized sorting (slices.Sort) and parallel bin sorts.
//   - Switchable hashing: xxh3 (default, faster).
//   - Optional CRLF stripping (can be disabled for LF-only datasets).
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"

	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

const (
	// Reader buffer used while indexing file1.
	readBufSize = 4 << 20 // 4 MiB

	// Buffered writer for stdout to amortize syscalls.
	writeBufSize = 4 << 20 // 4 MiB

	// Minimum byte range per worker when splitting file2.
	// Small ranges improve cache behavior on modest L3 caches.
	defaultBlkSz = 2 << 20 // 2 MiB

	defaultBufSz = 1 << 18 // 256KiB per worker
	defaultTmpSz = 64 << 9 // 32 KiB for temporary buffer

	// Default per-worker buffered output flush threshold (overridable via -flush).
	defaultOutputFlushSz = 2 << 20 // 2 MiB

	// We keep 48 bits (6 bytes) of xxhash for the set index.
	hashMask48 = 0xFFFFFFFFFFFF
)

// ReaderAt is what file2 must provide to parallel scanners.
// *os.File already satisfies this.
type ReaderAt interface {
	ReadAt(p []byte, off int64) (n int, err error)
}

type Size interface {
	Size() int64
}

// hashFn is selected at startup based on the -hash flag.
var hashFn func([]byte) uint64 = func(b []byte) uint64 { return xxh3.Hash(b) }

// scanOpts holds options for scanning.
type scanOpts struct {
	BufSz int // Buffer size for reading
	TmpSz int // Temporary buffer size for alignment and extension
}

// NewScanOpts returns default scan options if opts is nil.
func NewScanOpts(opts *scanOpts) *scanOpts {
	if opts == nil {
		return &scanOpts{
			BufSz: defaultBufSz,
			TmpSz: defaultTmpSz,
		}
	}
	return opts
}

// hash48b returns the low 48 bits of the selected hash of b.
func hash48b(b []byte) uint64 { return hashFn(b) & hashMask48 }

// stripCR optionally removes a trailing '\r' from a line (for CRLF).
// If strip == false, it returns b unchanged.
func stripCR(b []byte, strip bool) []byte {
	if !strip {
		return b
	}
	if n := len(b); n > 0 && b[n-1] == '\r' {
		return b[:n-1]
	}
	return b
}

// fastStripNL removes a trailing '\n' and returns a subslice aliasing b.
func fastStripNL(b []byte) []byte {
	n := len(b)
	if n > 0 && b[n-1] == '\n' {
		return b[:n-1]
	}
	return b
}

// Index16 is a compact two-level index over 48-bit hashes.
// The top 16 bits select a bin; within each bin, low 32-bit tails are sorted.
// Memory footprint is roughly 4 bytes * N (+ ~512 KiB for offsets).
type Index16 struct {
	// off has length 65,537. off[i]..off[i+1] bounds the i-th bin.
	off []int
	// data stores all low32 tails concatenated by bins.
	data []uint32
}

// binarySearch32 returns true if x is contained in sorted a.
// Hand-rolled to avoid generic overhead; hot in the lookup path.
func oldbinarySearch32(a []uint32, x uint32) bool {
	lo, hi := 0, len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1) // avoid overflow; branch-predictable
		v := a[mid]
		if v < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(a) && a[lo] == x
}

// Optimized binary search for small arrays using a more efficient approach
func binarySearch32(a []uint32, x uint32) bool {
	lo, hi := 0, len(a)-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if a[mid] == x {
			return true
		} else if a[mid] < x {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return false
}

// Contains reports whether the 48-bit hash h is present in the index.
// It performs a binary search within the selected bin. O(log bin_size).
func (idx *Index16) Contains(h uint64) bool {
	top := uint16(h >> 32)
	low := uint32(h & 0xFFFFFFFF)

	ti := int(top) // avoid uint16 wrap on +1
	lo, hi := idx.off[ti], idx.off[ti+1]
	if lo >= hi {
		return false
	}
	return binarySearch32(idx.data[lo:hi], low)
}

// buildIndex16Parallel constructs the compact index from file1 using two fully
// parallel streaming passes over newline-aligned byte ranges. It avoids atomics
// by assigning each worker disjoint subranges within every bin.
//
// Pass 1: each worker produces local counts[65536]; we reduce to global counts.
// Offsets: from global counts we compute off[65537], then per-worker starts.
// Pass 2: workers rescan and write low32 tails directly into data at their
//
//	per-bin start offsets, bumping a worker-local cursor.
//
// Finally: bins are sorted in place in parallel.
func buildIndex16Parallel(path string, stripCRLF bool, workers int, minBlk int64) (*Index16, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Best-effort kernel hints for large sequential scans.
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_WILLNEED)

	// Split file1 into W newline-aligned ranges.
	ranges, err := splitRanges(f, workers, minBlk)
	if err != nil {
		return nil, err
	}

	// ----- Pass 1: local counts per worker -----
	type wcounts = [1 << 16]int
	locals := make([]*wcounts, len(ranges))

	var wg sync.WaitGroup
	wg.Add(len(ranges))
	for wi, rg := range ranges {
		wi, rg := wi, rg
		go func() {
			defer wg.Done()
			const bufSz = 1 << 20
			rbuf := make([]byte, bufSz)
			var carry []byte
			c := new(wcounts)

			// Align range boundaries like scanRange does.
			// Start alignment
			if rg.start != 0 {
				tmp := make([]byte, 64<<10)
				pos := rg.start
				for {
					n, err := f.ReadAt(tmp, pos)
					if n == 0 {
						break
					}
					if i := bytes.IndexByte(tmp[:n], '\n'); i >= 0 {
						rg.start = pos + int64(i+1)
						break
					}
					pos += int64(n)
					if err == io.EOF {
						return
					}
				}
			}
			// End alignment
			if st, e := f.Stat(); e == nil {
				filesz := st.Size()
				if rg.end < filesz {
					tmp := make([]byte, 64<<10)
					pos := rg.end
					for {
						n, err := f.ReadAt(tmp, pos)
						if n == 0 {
							break
						}
						if j := bytes.IndexByte(tmp[:n], '\n'); j >= 0 {
							rg.end = pos + int64(j+1)
							break
						}
						pos += int64(n)
						if err == io.EOF {
							rg.end = filesz
							break
						}
					}
				}
			}

			// Scan range and count bins.
			for pos := rg.start; pos < rg.end; {
				toRead := bufSz
				if rem := int(rg.end - pos); rem < toRead {
					toRead = rem
				}
				n, err := f.ReadAt(rbuf[:toRead], pos)
				if n > 0 {
					data := rbuf[:n]
					if len(carry) > 0 {
						carry = append(carry, data...)
						data = carry
					}
					start := 0
					for i, ch := range data {
						if ch == '\n' {
							line := data[start:i]
							if len(line) != 0 {
								if stripCRLF && line[len(line)-1] == '\r' {
									line = line[:len(line)-1]
								}
								h := hash48b(line)
								(*c)[uint16(h>>32)]++
							}
							start = i + 1
						}
					}
					if start < len(data) {
						carry = append(carry[:0], data[start:]...)
					} else {
						carry = carry[:0]
					}
					pos += int64(n)
				}
				if err == io.EOF {
					break
				}
				if err != nil && err != io.EOF {
					return
				}
			}
			// Final unterminated line
			if len(carry) > 0 {
				line := carry
				if stripCRLF && len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
				if len(line) != 0 {
					h := hash48b(line)
					(*c)[uint16(h>>32)]++
				}
			}
			locals[wi] = c
		}()
	}
	wg.Wait()

	// Reduce local counts -> global counts and compute global offsets.
	counts := make([]int, 1<<16)
	for bin := 0; bin < (1 << 16); bin++ {
		sum := 0
		for _, lc := range locals {
			if lc != nil {
				sum += (*lc)[bin]
			}
		}
		counts[bin] = sum
	}
	off := make([]int, (1<<16)+1)
	total := 0
	for i := 0; i < (1 << 16); i++ {
		off[i] = total
		total += counts[i]
	}
	off[1<<16] = total

	// Compute per-worker starts for each bin: start[w][bin] = off[bin] + prefix_w(counts[w'][bin])
	starts := make([][]int, len(ranges))
	for w := range starts {
		starts[w] = make([]int, 1<<16)
	}
	for bin := 0; bin < (1 << 16); bin++ {
		base := off[bin]
		p := base
		for w, lc := range locals {
			starts[w][bin] = p
			if lc != nil {
				p += (*lc)[bin]
			}
		}
	}

	// Allocate the flat data slice and make worker-local cursors (copy of starts).
	data := make([]uint32, total)
	cursors := make([][]int, len(ranges))
	for w := range cursors {
		cpy := make([]int, 1<<16)
		copy(cpy, starts[w])
		cursors[w] = cpy
	}

	// ----- Pass 2: each worker fills its assigned slots, no atomics -----
	wg.Add(len(ranges))
	for wi, rg := range ranges {
		wi, rg := wi, rg
		go func() {
			defer wg.Done()
			const bufSz = 1 << 20
			rbuf := make([]byte, bufSz)
			var carry []byte
			cur := cursors[wi]

			// Re-align range as in Pass 1 (cheap; kept for clarity/correctness).
			if rg.start != 0 {
				tmp := make([]byte, 64<<10)
				pos := rg.start
				for {
					n, err := f.ReadAt(tmp, pos)
					if n == 0 {
						if err != nil {
							return
						}
						break
					}
					if i := bytes.IndexByte(tmp[:n], '\n'); i >= 0 {
						rg.start = pos + int64(i+1)
						break
					}
					pos += int64(n)
					if err == io.EOF {
						return
					}
				}
			}
			if st, e := f.Stat(); e == nil {
				filesz := st.Size()
				if rg.end < filesz {
					tmp := make([]byte, 64<<10)
					pos := rg.end
					for {
						n, err := f.ReadAt(tmp, pos)
						if n == 0 {
							break
						}
						if j := bytes.IndexByte(tmp[:n], '\n'); j >= 0 {
							rg.end = pos + int64(j+1)
							break
						}
						pos += int64(n)
						if err == io.EOF {
							rg.end = filesz
							break
						}
					}
				}
			}

			for pos := rg.start; pos < rg.end; {
				toRead := bufSz
				if rem := int(rg.end - pos); rem < toRead {
					toRead = rem
				}
				n, err := f.ReadAt(rbuf[:toRead], pos)
				if n > 0 {
					dataB := rbuf[:n]
					if len(carry) > 0 {
						carry = append(carry, dataB...)
						dataB = carry
					}
					start := 0
					for i, ch := range dataB {
						if ch == '\n' {
							line := dataB[start:i]
							if len(line) != 0 {
								if stripCRLF && line[len(line)-1] == '\r' {
									line = line[:len(line)-1]
								}
								h := hash48b(line)
								top := int(uint16(h >> 32))
								pos := cur[top]
								data[pos] = uint32(h & 0xFFFFFFFF)
								cur[top] = pos + 1
							}
							start = i + 1
						}
					}
					if start < len(dataB) {
						carry = append(carry[:0], dataB[start:]...)
					} else {
						carry = carry[:0]
					}
					pos += int64(n)
				}
				if err == io.EOF {
					break
				}
				if err != nil && err != io.EOF {
					return
				}
			}
			if len(carry) > 0 {
				line := carry
				if stripCRLF && len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
				if len(line) != 0 {
					h := hash48b(line)
					top := int(uint16(h >> 32))
					p := cur[top]
					data[p] = uint32(h & 0xFFFFFFFF)
					cur[top] = p + 1
				}
			}
		}()
	}
	wg.Wait()

	// ----- Parallel in-place sort per bin -----
	type task struct{ lo, hi int }
	tasks := make(chan task, 256)
	sortWorkers := runtime.GOMAXPROCS(0)
	if sortWorkers < 1 {
		sortWorkers = 1
	}
	wg.Add(sortWorkers)
	for w := 0; w < sortWorkers; w++ {
		go func() {
			defer wg.Done()
			for t := range tasks {
				slices.Sort(data[t.lo:t.hi])
			}
		}()
	}
	for i := 0; i < (1 << 16); i++ {
		lo, hi := off[i], off[i+1]
		if hi-lo > 1 {
			tasks <- task{lo: lo, hi: hi}
		}
	}
	close(tasks)
	wg.Wait()

	return &Index16{off: off, data: data}, nil
}

// buildIndex16 constructs the compact index from file1 using two streaming passes.
// Pass 1 counts items per top16 bin; Pass 2 fills a single flat array; finally,
// each bin is sorted in place. Sorting is parallelized across CPU cores.
//
// Low-hanging fruit applied:
//   - Use `slices.Sort`/`slices.BinarySearch` for type-specialized operations.
//   - Parallelize bin sorting with a worker pool limited to GOMAXPROCS.
//   - Reuse "carry" buffers to avoid allocations when handling very long lines.
//   - Provide kernel hints (fadvise) for better readahead on large sequential scans.
func buildIndex16(path string, stripCRLF bool) (*Index16, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Best-effort kernel hint: large sequential pass; please readahead.
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_WILLNEED)

	r := bufio.NewReaderSize(f, readBufSize)

	// Pass 1: count lines per top-16-bit bin.
	counts := make([]int, 1<<16) // 65,536 bins
	{
		var carry []byte
		for {
			chunk, err := r.ReadSlice('\n')
			if err == io.EOF {
				// Handle a final unterminated line.
				if len(chunk) > 0 {
					line := fastStripNL(append(carry, chunk...))
					if len(line) != 0 {
						line = stripCR(line, stripCRLF)
						h := hash48b(line)
						counts[uint16(h>>32)]++
					}
				} else if len(carry) > 0 {
					line := stripCR(carry, stripCRLF)
					if len(line) != 0 {
						h := hash48b(line)
						counts[uint16(h>>32)]++
					}
				}
				break
			}
			if err != nil && err != bufio.ErrBufferFull {
				return nil, err
			}
			// If the buffer filled before a newline, accumulate into carry.
			if err == bufio.ErrBufferFull {
				carry = append(carry, chunk...)
				continue
			}
			// We got a full line ending in '\n'.
			if len(carry) > 0 {
				carry = append(carry, chunk...)
				line := stripCR(fastStripNL(carry), stripCRLF)
				if len(line) != 0 {
					h := hash48b(line)
					counts[uint16(h>>32)]++
				}
				carry = carry[:0]
			} else {
				line := stripCR(fastStripNL(chunk), stripCRLF)
				if len(line) != 0 {
					h := hash48b(line)
					counts[uint16(h>>32)]++
				}
			}
		}
	}

	// Build offsets by prefix sum and total size.
	off := make([]int, (1<<16)+1)
	total := 0
	for i := 0; i < (1 << 16); i++ {
		off[i] = total
		total += counts[i]
	}
	off[1<<16] = total

	// Allocate contiguous storage for all low32 tails and a fill cursor.
	data := make([]uint32, total)
	cursor := make([]int, len(off))
	copy(cursor, off)

	// Rewind for Pass 2.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r.Reset(f)

	// Pass 2: fill per-bin regions in the flat array.
	{
		var carry []byte
		for {
			chunk, err := r.ReadSlice('\n')
			if err == io.EOF {
				if len(chunk) > 0 {
					line := stripCR(fastStripNL(append(carry, chunk...)), stripCRLF)
					if len(line) != 0 {
						h := hash48b(line)
						top := uint16(h >> 32)
						pos := cursor[top]
						data[pos] = uint32(h & 0xFFFFFFFF)
						cursor[top] = pos + 1
					}
				} else if len(carry) > 0 {
					line := stripCR(carry, stripCRLF)
					if len(line) != 0 {
						h := hash48b(line)
						top := uint16(h >> 32)
						pos := cursor[top]
						data[pos] = uint32(h & 0xFFFFFFFF)
						cursor[top] = pos + 1
					}
				}
				break
			}
			if err != nil && err != bufio.ErrBufferFull {
				return nil, err
			}
			if err == bufio.ErrBufferFull {
				carry = append(carry, chunk...)
				continue
			}
			if len(carry) > 0 {
				carry = append(carry, chunk...)
				line := stripCR(fastStripNL(carry), stripCRLF)
				if len(line) != 0 {
					h := hash48b(line)
					top := uint16(h >> 32)
					pos := cursor[top]
					data[pos] = uint32(h & 0xFFFFFFFF)
					cursor[top] = pos + 1
				}
				carry = carry[:0]
			} else {
				line := stripCR(fastStripNL(chunk), stripCRLF)
				if len(line) != 0 {
					h := hash48b(line)
					top := uint16(h >> 32)
					pos := cursor[top]
					data[pos] = uint32(h & 0xFFFFFFFF)
					cursor[top] = pos + 1
				}
			}
		}
	}

	// Sort each bin in place. We parallelize bins to utilize all cores.
	type task struct{ lo, hi int }
	tasks := make(chan task, 256)
	var wg sync.WaitGroup
	sortWorkers := runtime.GOMAXPROCS(0)
	if sortWorkers < 1 {
		sortWorkers = 1
	}
	wg.Add(sortWorkers)
	for w := 0; w < sortWorkers; w++ {
		go func() {
			defer wg.Done()
			for t := range tasks {
				slices.Sort(data[t.lo:t.hi]) // type-specialized sort on []uint32
			}
		}()
	}
	for i := 0; i < (1 << 16); i++ {
		lo, hi := off[i], off[i+1]
		if hi-lo > 1 {
			tasks <- task{lo: lo, hi: hi}
		}
	}
	close(tasks)
	wg.Wait()

	return &Index16{off: off, data: data}, nil
}

// fileRange denotes a half-open byte interval [start, end) within file2.
type fileRange struct{ start, end int64 }

// splitRanges divides file2 into roughly equal byte ranges, each at least
// minBlk bytes long. Workers later expand these to newline boundaries.
// Returns a slice of ranges covering the whole file.
func splitRanges(f *os.File, parts int, minBlk int64) ([]fileRange, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size == 0 {
		return nil, nil
	}
	if parts < 1 {
		parts = 1
	}
	chunk := size / int64(parts)
	if chunk < minBlk {
		chunk = minBlk
	}

	var ranges []fileRange
	var off int64
	for off < size {
		end := off + chunk
		if end > size {
			end = size
		}
		ranges = append(ranges, fileRange{start: off, end: end})
		off = end
	}
	return ranges, nil
}

// alignStart ensures that the range starts at a newline.
func alignStart(r ReaderAt, tmpSz int, start *int64) error {
	tmp := make([]byte, tmpSz)
	pos := *start
	for {
		n, err := r.ReadAt(tmp, pos)
		if n == 0 && err != nil {
			return err
		}
		if i := bytes.IndexByte(tmp[:n], '\n'); i >= 0 {
			*start = pos + int64(i+1)
			break
		}
		pos += int64(n)
		if err == io.EOF {
			return nil
		}
	}
	return nil
}

// alignEnd ensures that the range ends at a newline.
func alignEnd(r ReaderAt, tmpSz int, filesz int64, end *int64) error {
	if *end < filesz {
		tmp := make([]byte, tmpSz)
		pos := *end
		for {
			n, err := r.ReadAt(tmp, pos)
			if n == 0 && err != nil {
				break
			}
			if j := bytes.IndexByte(tmp[:n], '\n'); j >= 0 {
				*end = pos + int64(j+1)
				break
			}
			pos += int64(n)
			if err == io.EOF {
				*end = filesz
				break
			}
		}
	}
	return nil
}

// readAndProcessLines reads and processes lines from the file into the output buffer.
func XXreadAndProcessLines(
	r ReaderAt,
	buf []byte,
	carry *[]byte,
	pos, end int64,
	idx *Index16,
	stripCRLF bool,
	out *[]byte,
	flushSz int,
	flush func() error,
) (int, error) {
	toRead := len(buf)
	if rem := int(end - pos); rem < toRead {
		toRead = rem
	}

	n, err := r.ReadAt(buf[:toRead], pos)
	if n > 0 {
		data := buf[:n]
		if len(*carry) > 0 {
			*carry = append(*carry, data...)
			data = *carry
		}

		// Process each line
		start := 0
		for i, c := range data {
			if c == '\n' {
				line := data[start:i]
				if len(line) != 0 {
					line = stripCR(line, stripCRLF)
					h := hash48b(line)
					if !idx.Contains(h) {
						*out = append(*out, line...)
						*out = append(*out, '\n')
						if len(*out) >= flushSz {
							if err := flush(); err != nil {
								return n, err
							}
						}
					}
				}
				start = i + 1
			}
		}

		// Handle carryover bytes
		if start < len(data) {
			*carry = append((*carry)[:0], data[start:]...)
		} else {
			*carry = (*carry)[:0]
		}
	}
	return n, err
}

// --- main.go (after) ---
func scanRange(
	r ReaderAt,
	filesz int64,
	rg fileRange,
	idx *Index16,
	wr io.Writer,
	wmu *sync.Mutex,
	stripCRLF bool,
	flushSz int,
	opts *scanOpts,
) error {
	// Get scanOpts or use default
	options := NewScanOpts(opts)
	bufSz, tmpSz := options.BufSz, options.TmpSz

	buf := make([]byte, bufSz)
	out := make([]byte, 0, flushSz) // Reuse the output buffer for reduced allocations
	var carry []byte                // Reuse carry buffer to accumulate leftover bytes between reads

	// Helper function to flush output buffer to writer
	flush := func() error {
		if len(out) == 0 {
			return nil
		}
		wmu.Lock()
		_, err := wr.Write(out) // Flush the buffer when it's full or at EOF
		wmu.Unlock()
		out = out[:0] // Clear the output slice to prevent unnecessary memory usage
		return err
	}

	// Align to newline at the start (reused logic from previous version)
	if err := alignStart(r, tmpSz, &rg.start); err != nil {
		return err
	}

	// Extend the end to the next newline
	if err := alignEnd(r, tmpSz, filesz, &rg.end); err != nil {
		return err
	}

	// Scan the range and process lines
	pos := rg.start
	for pos < rg.end {
		n, err := readAndProcessLines(r, buf, &carry, pos, rg.end, idx, stripCRLF, &out, flushSz, flush)
		if err == io.EOF {
			// Expected when the reader reaches the end of file
			break
		} else if err != nil {
			return err
		}
		pos += int64(n)
	}

	// Final flush of remaining carryover bytes
	if len(carry) > 0 {
		line := stripCR(carry, stripCRLF)
		if len(line) != 0 {
			h := hash48b(line)
			if !idx.Contains(h) {
				out = append(out, line...)
				out = append(out, '\n')
			}
		}
	}

	return flush()
}

func readAndProcessLines(
	r ReaderAt,
	buf []byte,
	carry *[]byte,
	pos, end int64,
	idx *Index16,
	stripCRLF bool,
	out *[]byte,
	flushSz int,
	flush func() error,
) (int, error) {
	toRead := len(buf)
	if rem := int(end - pos); rem < toRead {
		toRead = rem
	}

	n, err := r.ReadAt(buf[:toRead], pos)
	if n > 0 {
		data := buf[:n]
		if len(*carry) > 0 {
			*carry = append(*carry, data...)
			data = *carry
		}

		// Process each line
		start := 0
		for i, c := range data {
			if c == '\n' {
				line := data[start:i]
				if len(line) != 0 {
					line = stripCR(line, stripCRLF)
					h := hash48b(line)
					if !idx.Contains(h) {
						*out = append(*out, line...) // Append to output buffer
						*out = append(*out, '\n')
						if len(*out) >= flushSz {
							if err := flush(); err != nil {
								return n, err
							}
						}
					}
				}
				start = i + 1
			}
		}

		// Handle leftover bytes that don't fit into a full line
		if start < len(data) {
			*carry = append((*carry)[:0], data[start:]...) // Reuse carry buffer
		} else {
			*carry = (*carry)[:0]
		}
	}
	return n, err
}

func main() {
	// Flags:
	//   -file1: baseline set of lines to compare against.
	//   -file2: candidate file; lines not present in file1 are emitted.
	//   -workers: number of parallel workers (default: GOMAXPROCS).
	//   -block: minimum byte range per worker (tune for cache/L3).
	//   -hash: "xxh3" (default).
	//   -strip_crlf: if false, skips CR stripping (slightly faster on LF-only data).
	//   -flush: per-worker flush size in bytes (reduces writer lock contention).
	file1 := flag.String("file1", "", "Path to the first file")
	file2 := flag.String("file2", "", "Path to the second file")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of worker goroutines for file2 processing")
	blockSize := flag.Int("block", defaultBlkSz, "Minimum byte range per worker (also used for alignment scanning)")
	hashAlg := flag.String("hash", "xxh3", `Hash algorithm: "xxh3" (default)`)
	stripCRLF := flag.Bool("strip_crlf", true, "Strip trailing '\\r' from lines (for CRLF). Disable for LF-only data.")
	flushSz := flag.Int("flush", defaultOutputFlushSz, "Per-worker buffered output flush threshold in bytes")
	flag.Parse()

	if *file1 == "" || *file2 == "" {
		fmt.Fprintln(os.Stderr, "Error: Both -file1 and -file2 are required.")
		flag.Usage()
		os.Exit(1)
	}
	// Select hash implementation.
	switch *hashAlg {
	case "xxh3":
		hashFn = func(b []byte) uint64 { return xxh3.Hash(b) }
	default:
		fmt.Fprintf(os.Stderr, "unknown -hash=%q (use xxh3)\n", *hashAlg)
		os.Exit(1)
	}

	// Reduce GC frequency during large one-shot runs, unless overridden by env.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(800)
	}

	// Build compact index from file1 (memory ~4 bytes/line + ~512 KiB offsets).
	// idx, err := buildIndex16(*file1, *stripCRLF)
	idx, err := buildIndex16Parallel(*file1, *stripCRLF, *workers, int64(*blockSize))

	if err != nil {
		fmt.Fprintf(os.Stderr, "build index: %v\n", err)
		os.Exit(1)
	}

	// Open file2 and split into many small byte ranges (not breaking lines).
	f2, err := os.Open(*file2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open file2: %v\n", err)
		os.Exit(1)
	}
	defer f2.Close()

	// Best-effort kernel hint: overall sequential access; let the kernel readahead.
	_ = unix.Fadvise(int(f2.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
	_ = unix.Fadvise(int(f2.Fd()), 0, 0, unix.FADV_WILLNEED)

	// Oversubscribe ranges to enable work stealing: many more ranges than workers.
	parts := *workers * 32
	if parts < *workers {
		parts = *workers
	}
	ranges, err := splitRanges(f2, parts, int64(*blockSize))
	if err != nil {
		fmt.Fprintf(os.Stderr, "split ranges: %v\n", err)
		os.Exit(1)
	}

	// Get file size once and pass it to workers (avoid per-worker Stat()).
	st2, err := f2.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat file2: %v\n", err)
		os.Exit(1)
	}
	filesz := st2.Size()

	// Shared buffered writer with a small critical section per flush.
	bw := bufio.NewWriterSize(os.Stdout, writeBufSize)

	// always emit the first line of file2 (assumed CSV header)
	f2h, err := os.Open(*file2)
	if err != nil { /* ... */
	}
	r2 := bufio.NewReader(f2h)
	header, err := r2.ReadBytes('\n')
	if err == nil && len(header) > 0 {
		bw.Write(header)
	}
	f2h.Close()

	var wmu sync.Mutex

	// Dynamic work-stealing pool: fixed number of workers consume many ranges.
	jobs := make(chan fileRange, len(ranges))
	for _, rg := range ranges {
		jobs <- rg
	}
	close(jobs)

	var wg sync.WaitGroup
	wg.Add(*workers)
	for w := 0; w < *workers; w++ {
		go func() {
			defer wg.Done()
			for rg := range jobs {
				// f2 is *os.File, which already implements ReaderAt
				if err := scanRange(f2, filesz, rg, idx, bw, &wmu, *stripCRLF, *flushSz, nil); err != nil {
					//				if err := scanRange(f2, filesz, rg, idx, bw, &wmu, *stripCRLF, *flushSz); err != nil {
					wmu.Lock()
					fmt.Fprintf(os.Stderr, "scanRange error [%d..%d): %v\n", rg.start, rg.end, err)
					wmu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	if err := bw.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}
}
