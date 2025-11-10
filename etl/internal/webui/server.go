package webui

import (
	"context"
	"embed"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"etl/internal/etl"
	"etl/internal/probe"
	sqlrepo "etl/internal/storage/sqlite"
)

//go:embed index.tmpl.html
var tplFS embed.FS

type Config struct {
	Addr string
}

type Server struct {
	cfg  Config
	tmpl *template.Template
	// runtime state
	lastConfig *etl.Config // latest edited config from UI
	dbPath     string
}

func NewServer(cfg Config) *http.Server {
	s := &Server{
		cfg:    cfg,
		dbPath: "./data.sqlite",
	}
	t, err := template.ParseFS(tplFS, "index.tmpl.html")
	if err != nil {
		panic(err)
	}
	s.tmpl = t

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/probe", s.handleProbe)        // POST
	mux.HandleFunc("/api/etl", s.handleETL)            // POST
	mux.HandleFunc("/api/schema", s.handleSchema)      // GET
	mux.HandleFunc("/api/sample", s.handleSampleField) // GET ?table=&field=
	mux.HandleFunc("/api/search", s.handleSearch)      // GET ?table=&q=
	mux.HandleFunc("/api/config", s.handleConfigEcho)  // POST/GET

	return &http.Server{
		Addr:    cfg.Addr,
		Handler: logRequests(mux),
	}
}

func logRequests(next http.Handler) http.Handler {
	// Simple request logger (method, path, duration).
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s (%s)", r.Method, r.URL.Path, time.Since(start))
	})
}

type probeReq struct {
	URL       string `json:"url"`
	Delimiter string `json:"delimiter"`
	Name      string `json:"name"`
	Bytes     int    `json:"bytes"`
	DatePref  string `json:"datepref"`
}

type probeResp struct {
	Config  etl.Config `json:"config"`
	Sample  [][]string `json:"sample,omitempty"`
	Message string     `json:"message,omitempty"`
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	_ = s.tmpl.Execute(w, nil)
}

// Probe: hit sampler to infer schema, return a default editable config
func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req probeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	delim := ','
	if req.Delimiter != "" {
		delim = rune(req.Delimiter[0])
	}
	maxBytes := req.Bytes
	if maxBytes == 0 {
		maxBytes = 20000
	}

	res, err := probe.Probe(probe.Options{
		URL:            req.URL,
		MaxBytes:       maxBytes,
		Delimiter:      delim,
		Name:           req.Name,
		OutputJSON:     true,
		DatePreference: req.DatePref,
		SaveSample:     false,
	})
	if err != nil {
		http.Error(w, "probe: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// The probe returns a full *pipeline* JSON (same as CLI).
	// Extract fields from transform.validate.contract.fields
	// and infer primary key from storage.*.key_columns (if present).
	var pipe struct {
		Transform []struct {
			Kind    string `json:"kind"`
			Options struct {
				Contract struct {
					Name   string `json:"name"`
					Fields []struct {
						Name     string `json:"name"`
						Type     string `json:"type"`
						Required bool   `json:"required"`
					} `json:"fields"`
				} `json:"contract"`
			} `json:"options"`
		} `json:"transform"`
		Parser struct {
			Options struct {
				HeaderMap map[string]string `json:"header_map"`
			} `json:"options"`
		} `json:"parser"`
		Storage struct {
			Kind     string `json:"kind"`
			Postgres struct {
				KeyColumns []string `json:"key_columns"`
			} `json:"postgres"`
			Sqlite struct {
				KeyColumns []string `json:"key_columns"`
			} `json:"sqlite"`
		} `json:"storage"`
	}

	if err := json.Unmarshal(res.Body, &pipe); err != nil {
		http.Error(w, "probe pipeline JSON: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Collect validated fields
	var fields []etl.Field
	for _, t := range pipe.Transform {
		if strings.ToLower(t.Kind) != "validate" {
			continue
		}
		for _, f := range t.Options.Contract.Fields {
			fields = append(fields, etl.Field{
				Name:     f.Name,
				Type:     mapPipelineTypeToUI(f.Type),
				Required: f.Required,
			})
		}
	}
	if len(fields) == 0 {
		http.Error(w, "probe: no fields found in transform.validate.contract.fields", http.StatusInternalServerError)
		return
	}

	// Determine primary key: prefer storage key_columns; otherwise pick a required integer-like field.
	primaryKey := ""
	if len(pipe.Storage.Postgres.KeyColumns) > 0 {
		primaryKey = pipe.Storage.Postgres.KeyColumns[0]
	} else if len(pipe.Storage.Sqlite.KeyColumns) > 0 {
		primaryKey = pipe.Storage.Sqlite.KeyColumns[0]
	} else {
		for _, f := range fields {
			switch strings.ToLower(f.Type) {
			case "integer", "int", "bigint":
				if f.Required {
					primaryKey = f.Name
					break
				}
			}
			if primaryKey != "" {
				break
			}
		}
	}

	cfg := etl.Config{
		Source: etl.Source{
			URL:       req.URL,
			Delimiter: req.Delimiter,
			Name:      req.Name,
		},
		Fields:     fields,
		PrimaryKey: primaryKey,
		HeaderMap:  pipe.Parser.Options.HeaderMap, // <-- pass through
	}

	s.lastConfig = &cfg
	writeJSON(w, probeResp{Config: cfg})
}

// Accept edited config from UI and run ETL
func (s *Server) handleETL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var cfg etl.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	if err := etl.Run(ctx, s.dbPath, cfg); err != nil {
		http.Error(w, "etl: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.lastConfig = &cfg
	writeJSON(w, map[string]any{
		"ok": true,
	})
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	if table == "" && s.lastConfig != nil {
		table = s.lastConfig.Source.Name
	}
	if table == "" {
		http.Error(w, "missing table", http.StatusBadRequest)
		return
	}
	db, err := sqlrepo.Open(s.dbPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()
	repo := sqlrepo.New(db)
	sqlText, err := repo.Schema(r.Context(), table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"ddl": sqlText})
}

func (s *Server) handleSampleField(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	field := r.URL.Query().Get("field")
	if table == "" && s.lastConfig != nil {
		table = s.lastConfig.Source.Name
	}
	if table == "" || field == "" {
		http.Error(w, "missing table/field", http.StatusBadRequest)
		return
	}
	limit := 10
	if l := r.URL.Query().Get("limit"); l != "" {
		if v := atoiSafe(l); v > 0 {
			limit = v
		}
	}
	db, err := sqlrepo.Open(s.dbPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()
	repo := sqlrepo.New(db)
	data, err := repo.SampleField(r.Context(), table, field, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"field": field,
		"rows":  data,
	})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	table := r.URL.Query().Get("table")
	if table == "" && s.lastConfig != nil {
		table = s.lastConfig.Source.Name
	}
	if table == "" {
		http.Error(w, "missing table", http.StatusBadRequest)
		return
	}

	db, err := sqlrepo.Open(s.dbPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()
	repo := sqlrepo.New(db)

	cols := []string{}
	if s.lastConfig != nil {
		for _, f := range s.lastConfig.Fields {
			cols = append(cols, f.Name)
		}
	}
	rows, err := repo.Search(r.Context(), table, cols, q, 50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	colsSQL, _ := rows.Columns()
	out := []map[string]any{}
	vals := make([]any, len(colsSQL))
	ptrs := make([]any, len(colsSQL))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		m := map[string]any{}
		for i, c := range colsSQL {
			m[c] = vals[i]
		}
		out = append(out, m)
	}
	writeJSON(w, map[string]any{
		"columns": colsSQL,
		"rows":    out,
	})
}

func (s *Server) handleConfigEcho(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if s.lastConfig == nil {
			writeJSON(w, map[string]any{})
			return
		}
		writeJSON(w, s.lastConfig)
	case http.MethodPost:
		var cfg etl.Config
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.lastConfig = &cfg
		writeJSON(w, map[string]any{"ok": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// --- helpers ---

func atoiSafe(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return i
}

func mapPipelineTypeToUI(t string) string {
	switch strings.ToLower(t) {
	case "int", "integer", "bigint", "smallint":
		return "integer"
	case "real", "double", "float", "numeric", "decimal":
		return "real"
	case "bool", "boolean":
		return "bool"
	case "date":
		return "date"
	case "timestamp", "timestamptz", "datetime":
		return "datetime"
	default:
		return "text"
	}
}
