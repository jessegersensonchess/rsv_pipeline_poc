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
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"sync"

	"github.com/zeebo/xxh3"
	"golang.org/x/sync/errgroup"
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

	// Access the bin's offset range
	ti := int(top)
	lo, hi := idx.off[ti], idx.off[ti+1]

	// If the bin is empty, return false immediately
	if lo >= hi {
		return false
	}

	// Perform a manual binary search for the hash in the bin range
	left, right := lo, hi-1
	for left <= right {
		mid := left + (right-left)>>1 // Avoid overflow in mid calculation
		midVal := idx.data[mid]

		if midVal == low {
			return true
		} else if midVal < low {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return false
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 64<<10) // 64KB buffer
	},
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
// MOVED func buildIndex16Parallel moved

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

// --- NEW: scanRange using per-worker context ---
// Returns a slice containing the lines from rg that are NOT in idx.
// The returned slice is safe for the caller to keep; the worker reuses its own buffers.

func scanRange(
	r ReaderAt,
	filesz int64,
	rg fileRange,
	idx *Index16,
	wc *WorkerCtx,
	opts Opts,
) ([]byte, error) {
	opts = opts.withDefaults()

	// Align boundaries to newlines
	if err := alignStart(r, opts.TmpSz, &rg.start); err != nil && err != io.EOF {
		return nil, err
	}
	if err := alignEnd(r, opts.TmpSz, filesz, &rg.end); err != nil && err != io.EOF {
		return nil, err
	}
	if rg.start >= rg.end {
		return nil, nil
	}

	buf := wc.Buf
	out := wc.Out[:0]
	wc.Carry = wc.Carry[:0]

	pos := rg.start
	for pos < rg.end {
		toRead := len(buf)
		if rem := int(rg.end - pos); rem < toRead {
			toRead = rem
		}
		n, err := r.ReadAt(buf[:toRead], pos)
		if n > 0 {
			data := buf[:n]
			if len(wc.Carry) > 0 {
				wc.Carry = append(wc.Carry, data...)
				data = wc.Carry
			}

			start := 0
			for i, c := range data {
				if c == '\n' {
					line := data[start:i]
					if len(line) != 0 {
						if opts.StripCRLF && line[len(line)-1] == '\r' {
							line = line[:len(line)-1]
						}
						if len(line) != 0 {
							if !idx.Contains(hash48b(line)) {
								out = append(out, line...)
								out = append(out, '\n')
								// Optional: local flush threshold to cap peak Out size
								//								if len(out) >= opts.FlushSz {
								// hand off partial chunk by copying into a fresh slice
								// (caller will aggregate; we keep out small)
								// We'll finish the copy in the worker loop to avoid many tiny chunks here.
								//								}
							}
						}
					}
					start = i + 1
				}
			}
			if start < len(data) {
				wc.Carry = append(wc.Carry[:0], data[start:]...)
			} else {
				wc.Carry = wc.Carry[:0]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		pos += int64(n)
	}

	// Final unterminated line
	if len(wc.Carry) > 0 {
		line := wc.Carry
		if opts.StripCRLF && len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if len(line) != 0 && !idx.Contains(hash48b(line)) {
			out = append(out, line...)
			out = append(out, '\n')
		}
	}

	// Hand off ownership: copy to a fresh slice so the worker can reuse its buffer next task.
	if len(out) == 0 {
		return nil, nil
	}
	result := make([]byte, len(out))
	copy(result, out)
	// keep grown capacity for reuse
	wc.Out = out[:0]
	return result, nil
}

func XXXscanRange(
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

// Define Range struct to represent the start and end positions in the file
type Range struct {
	start int64
	end   int64
}

// buildIndex16Parallel constructs the compact index from file1 using two fully
// parallel streaming passes over newline-aligned byte ranges. It avoids atomics
// by assigning each worker disjoint subranges within every bin.

// Corrected version of `alignRange` function to use `fileRange`
func alignRange(f *os.File, rg *fileRange, stripCRLF bool) error {
	// Start alignment
	if rg.start != 0 {
		tmp := bufPool.Get().([]byte)
		defer bufPool.Put(tmp)
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
				return nil
			}
		}
	}
	// End alignment
	if st, e := f.Stat(); e == nil {
		filesz := st.Size()
		if rg.end < filesz {
			tmp := bufPool.Get().([]byte)
			defer bufPool.Put(tmp)
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
	return nil
}

// Adjust the buildIndex16Parallel function to use `fileRange`

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

	// Memory pool to avoid allocations for each worker
	for wi, rg := range ranges {
		wi, rg := wi, rg
		go func() {
			defer wg.Done()
			// const bufSz = 1 << 20
			const bufSz = 1 << 18
			rbuf := make([]byte, bufSz)
			var carry []byte
			c := new(wcounts)

			// Read the aligned start and end positions for the range
			if err := alignRange(f, &rg, stripCRLF); err != nil {
				return
			}

			// Scan range and count bins
			for pos := rg.start; pos < rg.end; {
				toRead := int(rg.end - pos)
				if toRead > len(rbuf) {
					toRead = len(rbuf)
				}
				n, err := f.ReadAt(rbuf[:toRead], pos)
				if err != nil && err != io.EOF {
					return
				}
				if n == 0 {
					break
				}

				data := rbuf[:n]
				if len(carry) > 0 {
					carry = append(carry, data...)
					data = carry
				}

				// Process the data in chunks, counting bins
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

			// Save the local counts for this worker
			locals[wi] = c
		}()
	}
	wg.Wait()

	// Reduce local counts -> global counts and compute global offsets
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

	// Allocate the flat data slice and make worker-local cursors (copy of starts)
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

			// Reuse buffer from the pool for reading
			rbuf := bufPool.Get().([]byte)
			defer bufPool.Put(rbuf)

			var carry []byte
			cur := cursors[wi]

			// Re-align range as in Pass 1 (cheap; kept for clarity/correctness)
			if err := alignRange(f, &rg, stripCRLF); err != nil {
				return
			}

			// Scan range and fill the assigned slots in the data slice
			for pos := rg.start; pos < rg.end; {
				toRead := int(rg.end - pos)
				if toRead > len(rbuf) {
					toRead = len(rbuf)
				}
				n, err := f.ReadAt(rbuf[:toRead], pos)
				if err != nil && err != io.EOF {
					return
				}
				if n == 0 {
					break
				}

				dataB := rbuf[:n]
				if len(carry) > 0 {
					carry = append(carry, dataB...)
					dataB = carry
				}

				// Process the data and fill in the bins for each worker
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

			// Final unterminated line
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
				sort.Slice(data[t.lo:t.hi], func(i, j int) bool {
					return data[t.lo+i] < data[t.lo+j]
				})
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

// --- NEW: options and per-worker scratch ---

type Opts struct {
	StripCRLF bool
	FlushSz   int // per-chunk flush threshold for workers (not shared writer buf)
	BufSz     int // read buffer
	TmpSz     int // alignment scratch
}

func (o *Opts) withDefaults() Opts {
	out := *o
	if out.BufSz == 0 {
		out.BufSz = defaultBufSz
	}
	if out.TmpSz == 0 {
		out.TmpSz = defaultTmpSz
	}
	if out.FlushSz == 0 {
		out.FlushSz = defaultOutputFlushSz
	}
	return out
}

type WorkerCtx struct {
	Buf   []byte // read buffer
	Carry []byte // partial line across reads
	Out   []byte // reusable output buffer
}

var workerPool = sync.Pool{
	New: func() any {
		return &WorkerCtx{
			Buf: make([]byte, defaultBufSz),
			Out: make([]byte, 0, defaultOutputFlushSz),
		}
	},
}

// runFile2 processes file2 with worker pool producing []byte chunks and a single
// writer goroutine that batches to stdout. Avoids deadlock by closing `results`
// when (and only when) the workers are done.
func runFile2(f2 *os.File, idx *Index16, workers int, minBlk int64, opts Opts) error {
	opts = opts.withDefaults()

	// Split work
	st2, err := f2.Stat()
	if err != nil {
		return fmt.Errorf("stat file2: %w", err)
	}
	filesz := st2.Size()

	ranges, err := splitRanges(f2, workers*32, minBlk)
	if err != nil {
		return fmt.Errorf("split ranges: %w", err)
	}

	type result struct {
		data []byte
		err  error
	}

	jobs := make(chan fileRange, len(ranges))
	results := make(chan result, workers*2)

	for _, rg := range ranges {
		jobs <- rg
	}
	close(jobs)

	// One errgroup for the writer (provides cancellation context).
	gWriter, ctx := errgroup.WithContext(context.Background())

	// Writer goroutine: only place that touches stdout.
	gWriter.Go(func() error {
		bw := bufio.NewWriterSize(os.Stdout, writeBufSize)
		defer bw.Flush()

		// Emit header once (best-effort).
		if name := f2.Name(); name != "" {
			if fh, e := os.Open(name); e == nil {
				br := bufio.NewReader(fh)
				if header, e2 := br.ReadBytes('\n'); e2 == nil && len(header) > 0 {
					if _, e3 := bw.Write(header); e3 != nil {
						_ = fh.Close()
						return e3
					}
				}
				_ = fh.Close()
			}
		}

		for r := range results {
			if r.err != nil {
				return r.err
			}
			if len(r.data) == 0 {
				continue
			}
			if _, err := bw.Write(r.data); err != nil {
				return err
			}
		}
		return nil
	})

	// Separate group for workers. When workers are done, we close `results`.
	gWorkers, _ := errgroup.WithContext(ctx) // inherit ctx for cancellation
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	// Optionally cap concurrency here:
	// gWorkers.SetLimit(workers) // if you want to use more tasks than goroutines

	for i := 0; i < workers; i++ {
		gWorkers.Go(func() error {
			wc := workerPool.Get().(*WorkerCtx)
			defer workerPool.Put(wc)

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rg, ok := <-jobs:
					if !ok {
						return nil
					}
					data, err := scanRange(f2, filesz, rg, idx, wc, opts)
					select {
					case results <- result{data: data, err: err}:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		})
	}

	// Close `results` when all workers finish (success or error).
	done := make(chan struct{})
	go func() {
		_ = gWorkers.Wait() // ignore here; we return error below
		close(results)
		close(done)
	}()

	// Wait for writer to finish (it will stop on error or after results closes).
	if err := gWriter.Wait(); err != nil && err != context.Canceled {
		<-done // ensure results closer finished to avoid leaks
		return err
	}

	// If writer was fine, check worker errors.
	if err := gWorkers.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func main() {
	// Flags:
	//   -file1: baseline set of lines to compare against.
	//   -file2: candidate file; lines not present in file1 are emitted.
	//   -workers: number of parallel workers (default: GOMAXPROCS).
	//   -block: minimum byte range per worker (tune for cache/L3).
	//   -hash: "xxh3" (default).
	//   -strip_crlf: if false, skips CR stripping (slightly faster on LF-only data).
	//   -flush: per-worker flush size in bytes (caps worker buffer growth).
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
	idx, err := buildIndex16Parallel(*file1, *stripCRLF, *workers, int64(*blockSize))
	if err != nil {
		fmt.Fprintf(os.Stderr, "build index: %v\n", err)
		os.Exit(1)
	}

	// Open file2; run the parallel scan+writer pipeline.
	f2, err := os.Open(*file2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open file2: %v\n", err)
		os.Exit(1)
	}
	defer f2.Close()

	// Best-effort kernel hint: overall sequential access; let the kernel readahead.
	_ = unix.Fadvise(int(f2.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
	_ = unix.Fadvise(int(f2.Fd()), 0, 0, unix.FADV_WILLNEED)

	// Run the new pipeline. It:
	//   * splits ranges,
	//   * fans out to workers (with per-worker reusable buffers),
	//   * funnels results to a single writer goroutine (batched stdout).
	if err := runFile2(
		f2,
		idx,
		*workers,
		int64(*blockSize),
		Opts{
			StripCRLF: *stripCRLF,
			FlushSz:   *flushSz,
			BufSz:     defaultBufSz,
			TmpSz:     defaultTmpSz,
		},
	); err != nil {
		fmt.Fprintf(os.Stderr, "file2 processing error: %v\n", err)
		os.Exit(1)
	}
}
