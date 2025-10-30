package vehicletech

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"csvloader/internal/config"
	"csvloader/internal/csvutil"
	"csvloader/internal/db"
	"csvloader/internal/jsonutil"
	"csvloader/internal/pcv"
	// Optional MSSQL driver (used by the generic SQL adapter when --db_driver=mssql)
)

var skippedRoot = "skipped"

// WriterBackend performs the streaming write of encoded jobs.
type WriterBackend interface {
	// Write drains encodedCh and performs the bulk write.
	// It must call addSkip for any rejected row.
	Write(ctx context.Context, encodedCh <-chan encodedJob, addSkip func(reason string, ln int, pcvField, raw string), logEvery int) (inserted int, err error)
}

// WriterBackendFactory produces a backend per writer goroutine.
type WriterBackendFactory func(ctx context.Context) (WriterBackend, error)

type rawJob struct {
	line    string
	lineNum int
}

type parsedJob struct {
	fields  []string
	lineNum int
	raw     string
}

type encodedJob struct {
	pcv      int64
	pcvField string
	payload  []byte
	lineNum  int
	raw      string
}

type VehicleTechPipeline struct {
	ctx            context.Context
	cfg            *config.Config
	factory        db.DBFactory
	backendFactory WriterBackendFactory
	headers        []string
	pcvIdx         int
	statusIdx      int
	jsonEnc        *jsonutil.FastJSONEncoder
	isPG           bool

	// channels
	rawCh     chan rawJob
	parsedCh  chan parsedJob
	encodedCh chan encodedJob
	errCh     chan error

	// workers
	parserWorkers  int
	encoderWorkers int
	initialWriters int
	maxWriters     int

	// sync
	parseWG sync.WaitGroup
	encWG   sync.WaitGroup
	writeWG sync.WaitGroup

	activeWriters atomic.Int32
	digitsOnly    *regexp.Regexp
}

func newVehicleTechPipeline(ctx context.Context, cfg *config.Config, factory db.DBFactory, headers []string, pcvIdx, statusIdx int) *VehicleTechPipeline {
	p := &VehicleTechPipeline{
		ctx:            ctx,
		cfg:            cfg,
		factory:        factory,
		headers:        headers,
		pcvIdx:         pcvIdx,
		statusIdx:      statusIdx,
		jsonEnc:        jsonutil.NewFastJSONEncoder(headers),
		isPG:           strings.ToLower(cfg.DBDriver) == "postgres",
		rawCh:          make(chan rawJob, 64*1024),
		parsedCh:       make(chan parsedJob, 8*1024),
		encodedCh:      make(chan encodedJob, 8*1024),
		errCh:          make(chan error, 4),
		parserWorkers:  cfg.Workers,
		encoderWorkers: max(4, cfg.Workers/2), // was 1
		initialWriters: max(4, cfg.Workers/4), // was 1
		maxWriters:     max(8, cfg.Workers),
		digitsOnly:     regexp.MustCompile(`^\d+$`),
	}
	// Default production backends:
	if p.isPG {
		p.backendFactory = func(ctx context.Context) (WriterBackend, error) {
			dbc, err := factory(ctx)
			if err != nil {
				return nil, err
			}
			return newPgWriterBackend(dbc), nil
		}
	} else {
		p.backendFactory = func(ctx context.Context) (WriterBackend, error) {
			dbc, err := factory(ctx)
			if err != nil {
				return nil, err
			}
			return newMSSQLWriterBackend(dbc), nil
		}
	}
	p.activeWriters.Store(int32(p.initialWriters))
	return p
}

var newVehicleTechPipelineHook = newVehicleTechPipeline

func (p *VehicleTechPipeline) startReader(r *bufio.Reader) {
	go func() {
		defer close(p.rawCh)
		lineNum := 1 // header consumed
		for {
			l, err := csvutil.ReadLogicalCSVLine(r)
			if err == io.EOF {
				return
			}
			if err != nil {
				p.errCh <- fmt.Errorf("tech read error: %w", err)
				return
			}
			lineNum++
			p.rawCh <- rawJob{line: l, lineNum: lineNum}
		}
	}()
}

func (p *VehicleTechPipeline) startParsers() {
	p.parseWG.Add(p.parserWorkers)
	for i := 0; i < p.parserWorkers; i++ {
		go func() {
			defer p.parseWG.Done()
			for j := range p.rawCh {
				fields, err := csvutil.ParseCSVLineLoose(j.line)
				if err != nil {
					p.parsedCh <- parsedJob{fields: nil, lineNum: j.lineNum, raw: j.line}
					continue
				}
				p.parsedCh <- parsedJob{fields: fields, lineNum: j.lineNum, raw: j.line}
			}
		}()
	}
	go func() {
		p.parseWG.Wait()
		close(p.parsedCh)
	}()
}

func (p *VehicleTechPipeline) startEncoders() {
	p.encWG.Add(p.encoderWorkers)
	for i := 0; i < p.encoderWorkers; i++ {
		go func() {
			defer p.encWG.Done()
			for j := range p.parsedCh {
				if j.fields == nil {
					p.encodedCh <- encodedJob{pcv: 0, pcvField: "", payload: nil, lineNum: j.lineNum, raw: j.raw}
					continue
				}
				fields := j.fields

				// PCV extraction with RSV fallback (handles poorly formatted lines)
				pcv, pcvField := pcv.ExtractPCVWithRSVFallback(
					p.headers, fields, p.pcvIdx, p.statusIdx, p.digitsOnly, j.raw,
				)
				if pcv == 0 {
					p.encodedCh <- encodedJob{pcv: 0, pcvField: pcvField, payload: nil, lineNum: j.lineNum, raw: j.raw}
					continue
				}

				// normalize field count
				switch {
				case len(fields) > len(p.headers):
					fields = fields[:len(p.headers)]
				case len(fields) < len(p.headers):
					fields = append(fields, make([]string, len(p.headers)-len(fields))...)
				}

				js := p.jsonEnc.EncodeRow(fields)
				p.encodedCh <- encodedJob{pcv: pcv, pcvField: pcvField, payload: js, lineNum: j.lineNum, raw: j.raw}
			}
		}()
	}
	go func() {
		p.encWG.Wait()
		close(p.encodedCh)
	}()
}

func (p *VehicleTechPipeline) writerFn(id int, statsCh chan<- writerStats) {
	defer p.writeWG.Done()
	res := writerStats{reasons: map[string]int{}}

	// per-writer skipped CSV
	if err := os.MkdirAll(skippedRoot, 0o755); err != nil {
		res.err = fmt.Errorf("writer %d create skipped dir: %w", id, err)
		statsCh <- res
		return
	}
	skf, err := os.Create(filepath.Join(skippedRoot, fmt.Sprintf("skipped_vehicle_tech_w%d.csv", id)))
	if err != nil {
		res.err = fmt.Errorf("writer %d skipped file: %w", id, err)
		statsCh <- res
		return
	}
	defer skf.Close()
	skw := csv.NewWriter(skf)
	defer skw.Flush()
	_ = skw.Write([]string{"reason", "line_number", "pcv_field", "raw_line"})
	addSkip := func(reason string, ln int, pcvField, raw string) {
		res.reasons[reason]++
		res.skipped++
		_ = skw.Write([]string{reason, strconv.Itoa(ln), pcvField, raw})
	}

	// Progress logging cadence
	logEvery := 5_000
	if p.cfg.BatchSize > 0 && p.cfg.BatchSize*5 > logEvery {
		logEvery = p.cfg.BatchSize * 5
	}

	// Create a backend for this writer and stream
	backend, err := p.backendFactory(p.ctx)
	if err != nil {
		res.err = fmt.Errorf("writer %d backend: %w", id, err)
		statsCh <- res
		return
	}

	inserted, err := backend.Write(p.ctx, p.encodedCh, addSkip, logEvery)
	if err != nil {
		res.err = fmt.Errorf("writer %d write: %w", id, err)
		statsCh <- res
		return
	}
	res.inserted += inserted
	log.Printf("vehicle_tech[w%d]: inserted=%d skipped=%d", id, res.inserted, res.skipped)
	statsCh <- res
}

func (p *VehicleTechPipeline) startWritersAndAutoscale(statsCh chan<- writerStats) {
	// initial pool
	for i := 0; i < p.initialWriters; i++ {
		p.writeWG.Add(1)
		go p.writerFn(i+1, statsCh)
	}

	// autoscale: if encodedCh >80% full, add a writer up to maxWriters
	go func() {
		tick := time.NewTicker(3 * time.Second)
		defer tick.Stop()
		for range tick.C {
			qlen, capQ := len(p.encodedCh), cap(p.encodedCh)
			if capQ == 0 {
				continue
			}
			fill := float64(qlen) / float64(capQ)
			curr := int(p.activeWriters.Load())
			if fill > 0.80 && curr < p.maxWriters {
				newTotal := int(p.activeWriters.Add(1))
				p.writeWG.Add(1)
				go p.writerFn(newTotal, statsCh)
				log.Printf("⚙️ autoscale: added writer w%d (queue %.0f%% full, writers=%d)", newTotal, fill*100, newTotal)
			}
		}
	}()
}

func (p *VehicleTechPipeline) waitCloseStats(statsCh chan writerStats) (totalInserted, totalSkipped int, reasons map[string]int, firstErr error) {
	// close stats when all writers done
	go func() {
		p.writeWG.Wait()
		close(statsCh)
	}()

	reasons = map[string]int{}
	for st := range statsCh {
		totalInserted += st.inserted
		totalSkipped += st.skipped
		if st.err != nil && firstErr == nil {
			firstErr = st.err
		}
		for k, v := range st.reasons {
			reasons[k] += v
		}
	}
	return
}

func ImportVehicleTech(ctx context.Context, cfg *config.Config, factory db.DBFactory, path string) error {
	start := time.Now()
	// Ensure table
	db, err := factory(ctx)
	if err != nil {
		return fmt.Errorf("open db (ensure table): %w", err)
	}
	if err := ensureVehicleTechTable(ctx, db, cfg.UnloggedTables, cfg.DBDriver); err != nil {
		_ = db.Close(ctx)
		return err
	}
	_ = db.Close(ctx)

	// Open & read headers
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open tech csv: %w", err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 32<<20)
	headerLine, err := csvutil.ReadLogicalCSVLine(r)
	if err != nil {
		return fmt.Errorf("read tech header: %w", err)
	}
	headers, perr := csvutil.ParseCSVLineLoose(headerLine)
	if perr != nil {
		return fmt.Errorf("parse tech header: %w", perr)
	}
	pcvIdx := pcv.FindPCVIndex(headers)
	if pcvIdx < 0 {
		return fmt.Errorf("PČV column not found in header")
	}
	statusIdx := -1
	for i, h := range headers {
		if strings.TrimSpace(h) == "Status" {
			statusIdx = i
			break
		}
	}

	// Build pipeline
	//	p := newVehicleTechPipeline(ctx, cfg, factory, headers, pcvIdx, statusIdx)
	p := newVehicleTechPipelineHook(ctx, cfg, factory, headers, pcvIdx, statusIdx)

	// Stages
	p.startReader(r)
	p.startParsers()
	p.startEncoders()

	// Writers + autoscaling
	statsCh := make(chan writerStats, p.maxWriters)
	p.startWritersAndAutoscale(statsCh)

	// Capture early read error (non-fatal if writers succeed)
	var earlyErr error
	select {
	case earlyErr = <-p.errCh:
	default:
	}

	// Aggregate
	inserted, skipped, reasons, firstErr := p.waitCloseStats(statsCh)
	if firstErr != nil {
		return firstErr
	}
	if earlyErr != nil {
		log.Printf("⚠️ early pipeline warning: %v", earlyErr)
	}

	// Log summary with actual writer count
	var parts []string
	for k, v := range reasons {
		parts = append(parts, fmt.Sprintf("%s=%d", k, v))
	}
	duration := time.Since(start)
	processingRate := float64(inserted) / duration.Seconds()

	log.Printf(
		"vehicle_tech (pipeline: parsers=%d encoders=%d writers=%d): inserted=%d skipped=%d (%s), duration=%s, rate_per_second=%.0f",
		p.parserWorkers, p.encoderWorkers, int(p.activeWriters.Load()),
		inserted, skipped, strings.Join(parts, ", "), duration.Round(time.Millisecond), processingRate,
	)

	return nil
}

// writerBackendNoop drains the channel (which will be empty here) and reports zero inserts.
type writerBackendNoop struct{}

func (writerBackendNoop) Write(ctx context.Context, encodedCh <-chan encodedJob, _ func(string, int, string, string), _ int) (int, error) {
	for range encodedCh { /* drain if anything sneaks in */
	}
	return 0, nil
}
