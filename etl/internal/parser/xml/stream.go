package xmlparser

import (
	"bufio"
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"log"
	"sync"
)

// ParseStream concurrently parses records from src and sends Record objects to
// 'out'. It only emits fully-closed records and tolerates truncated inputs.
// If opts.ZeroCopy is true, 'data' must hold the entire XML bytes; otherwise
// 'r' is used for streaming sharding.
//
// The function returns when EOF/truncation is reached, on context cancel,
// or on a fatal error.

func ParseStream(
	ctx context.Context,
	r io.Reader, // used when ZeroCopy=false
	data []byte, // used when ZeroCopy=true
	recordTag string,
	comp Compiled,
	opts Options,
	out chan<- Record,
) error {
	jobs := make(chan Job, opts.Queue)
	results := make(chan Result, opts.Queue)

	// Start workers
	var wg sync.WaitGroup
	wcnt := opts.Workers
	if wcnt <= 0 {
		wcnt = 1
	}
	wg.Add(wcnt)
	for i := 0; i < wcnt; i++ {
		go func(workerID int) {
			defer wg.Done()
			if opts.Debug {
				log.Printf("Worker %d started", workerID)
			}
			workerLoop(comp, opts.UltraFast, jobs, results)
			if opts.Debug {
				log.Printf("Worker %d finished", workerID)
			}
		}(i)
	}

	// Sharder
	feedErr := make(chan error, 1)
	go func() {
		defer close(jobs) // Close jobs channel once all jobs are dispatched
		log.Println("Feeding jobs to workers...")
		if opts.ZeroCopy {
			feedErr <- ShardZeroCopy(data, recordTag, jobs)
		} else {
			feedErr <- ShardStreaming(r, recordTag, opts.BufSize, jobs)
		}
	}()

	// Closing results after workers finish
	done := make(chan struct{})
	go func() {
		wg.Wait() // Wait for all workers to finish
		log.Println("All workers finished")
		close(results)
		close(done)
	}()

	// Optional reordering buffer
	expect := 0
	reorder := make(map[int]Record)

	// Main loop for reading results from workers
	for {
		select {
		case <-ctx.Done():
			if opts.Debug {
				log.Println("Context done, terminating...")
			}
			<-done
			return ctx.Err()

		case res, ok := <-results:
			if !ok {
				if opts.Debug {
					log.Println("Results channel closed, workers are done")
				}
				// If results channel is closed, workers are done
				return <-feedErr
			}
			if res.Err != nil {
				if opts.Debug {
					log.Printf("Error in worker: %v", res.Err)
				}
				// Handle any errors from workers
				continue
			}

			if opts.PreserveOrder || opts.OrderWindow > 0 {
				reorder[res.Index] = res.Record
				for {
					rec, ok := reorder[expect]
					if !ok {
						break
					}
					select {
					case out <- rec:
						if opts.Debug {
							log.Printf("Emitting record %d in order", expect)
						}
					case <-ctx.Done():
						<-done
						return ctx.Err()
					}
					delete(reorder, expect)
					expect++
				}
				continue
			}

			// Unordered output - faster
			select {
			case out <- res.Record:
				//				log.Println("Emitting unordered record")
			case <-ctx.Done():
				<-done
				return ctx.Err()
			}
		}
	}
}

func workerLoop(comp Compiled, useUltra bool, jobs <-chan Job, results chan<- Result) {
	for j := range jobs {
		rec, err := parseOneRecord(j.Bytes, comp, useUltra)
		results <- Result{Index: j.Index, Record: rec, Err: err}
	}
}

// parseOneRecord parses a single <record_tag> blob using either the config
// matcher or an optional ultrafast schema fast-path.
func parseOneRecord(b []byte, comp Compiled, useUltra bool) (Record, error) {
	if useUltra {
		return UltraFastExtractPubMed(b)
	}
	// config-driven path
	br := bufio.NewReaderSize(bytes.NewReader(b), 64<<10)
	dec := xml.NewDecoder(br)
	dec.Strict = false

	record := make(Record, 8)
	inRecord := false
	var rel []string
	type capture struct {
		key   string
		list  bool
		depth int
		text  []byte
	}
	var caps []capture

	for {
		tok, err := dec.Token()
		if err != nil {
			if err == io.EOF || isTruncErr(err) {
				return record, nil
			}
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if !inRecord {
				if t.Name.Local == comp.recordTag {
					inRecord = true
					rel = rel[:0]
					caps = caps[:0]
				}
				continue
			}
			rel = append(rel, t.Name.Local)
			if cands := comp.byLast[t.Name.Local]; len(cands) > 0 {
				for _, m := range cands {
					if !tailMatches(rel, m.spec) {
						continue
					}
					ls := m.spec.segs[len(m.spec.segs)-1]
					ok := true
					if ls.attrName != "" {
						ok = false
						for _, a := range t.Attr {
							if a.Name.Local == ls.attrName && a.Value == ls.attrVal {
								ok = true
								break
							}
						}
					}
					if !ok {
						continue
					}
					caps = append(caps, capture{
						key:   m.outKey,
						list:  m.isList,
						depth: len(rel),
						text:  make([]byte, 0, 64),
					})
				}
			}
		case xml.CharData:
			if inRecord && len(caps) > 0 {
				for i := range caps {
					caps[i].text = append(caps[i].text, t...)
				}
			}
		case xml.EndElement:
			if !inRecord {
				continue
			}
			// commit captures that end at this depth
			if len(caps) > 0 {
				w := 0
				for _, cp := range caps {
					if cp.depth == len(rel) {
						val := stringsTrimSpace(cp.text)
						if len(val) > 0 {
							if cp.list {
								if arr, ok := record[cp.key].([]string); ok {
									record[cp.key] = append(arr, string(val))
								} else {
									record[cp.key] = []string{string(val)}
								}
							} else if _, exists := record[cp.key]; !exists {
								record[cp.key] = string(val)
							}
						}
					} else {
						caps[w] = cp
						w++
					}
				}
				caps = caps[:w]
			}
			if t.Name.Local == comp.recordTag && len(rel) == 0 {
				return record, nil
			}
			if len(rel) > 0 {
				rel = rel[:len(rel)-1]
			}
		}
	}
}

func stringsTrimSpace(b []byte) []byte {
	// ASCII-focused trim to avoid allocating string, then []byte again.
	i, j := 0, len(b)-1
	for i <= j && (b[i] == ' ' || b[i] == '\n' || b[i] == '\r' || b[i] == '\t') {
		i++
	}
	for j >= i && (b[j] == ' ' || b[j] == '\n' || b[j] == '\r' || b[j] == '\t') {
		j--
	}
	if i > j {
		return nil
	}
	return b[i : j+1]
}
