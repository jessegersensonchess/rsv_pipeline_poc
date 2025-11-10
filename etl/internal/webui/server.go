// Package webui exposes a minimal HTTP server with an HTML form that
// lets you test/run the CSV sampler and see results rendered as either
// CSV (header,name,type) lines or as a JSON config blob.
//
// Routes:
//
//	GET  /          → form
//	POST /probe     → runs probe with form inputs; renders output inline
//	GET  /api/probe → machine-friendly API, returns text/plain (CSV or JSON)
package webui

import (
	_ "embed"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"

	"csv_header_fetcher/internal/probe"
)

// Config controls server startup.
type Config struct {
	Addr string
}

// Server wraps http.Server for convenience.
type Server struct {
	cfg  Config
	mux  *http.ServeMux
	tmpl *template.Template
}

// NewServer constructs a Server with routes and embedded template.
func NewServer(cfg Config) *Server {
	s := &Server{
		cfg: cfg,
		mux: http.NewServeMux(),
		// Parse the embedded template at init time.
		tmpl: template.Must(template.New("index").Parse(indexHTML)),
	}
	s.routes()
	return s
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe() error {
	return http.ListenAndServe(s.cfg.Addr, s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/", s.handleIndex)
	s.mux.HandleFunc("/probe", s.handleProbe)
	s.mux.HandleFunc("/api/probe", s.handleAPIProbe)
}

// handleIndex renders the input form.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	_ = s.tmpl.Execute(w, nil)
}

// handleProbe processes the form and renders a results page.
func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form: "+err.Error(), http.StatusBadRequest)
		return
	}
	url := strings.TrimSpace(r.FormValue("url"))
	name := strings.TrimSpace(r.FormValue("name"))
	bytesStr := strings.TrimSpace(r.FormValue("bytes"))
	delimiter := r.FormValue("delimiter")
	mode := r.FormValue("mode") // "csv" or "json"
	datePref := r.FormValue("date_pref")

	nbytes, _ := strconv.Atoi(bytesStr)
	opt := probe.Options{
		URL:            url,
		MaxBytes:       nbytes,
		Delimiter:      probe.DecodeDelimiter(delimiter),
		Name:           name,
		OutputJSON:     mode == "json",
		DatePreference: datePref,
	}
	res, err := probe.Probe(opt)
	if err != nil {
		http.Error(w, "probe failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	data := struct {
		URL        string
		Name       string
		Bytes      int
		Delimiter  string
		Mode       string
		DatePref   string
		ResultText string
	}{
		URL:        url,
		Name:       name,
		Bytes:      nbytes,
		Delimiter:  delimiter,
		Mode:       mode,
		DatePref:   datePref,
		ResultText: string(res.Body),
	}
	if err := s.tmpl.Execute(w, data); err != nil {
		log.Println("template error:", err)
	}
}

// handleAPIProbe returns text/plain so scripts can curl it easily.
func (s *Server) handleAPIProbe(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	url := strings.TrimSpace(q.Get("url"))
	name := strings.TrimSpace(q.Get("name"))
	bytesStr := strings.TrimSpace(q.Get("bytes"))
	delimiter := q.Get("delimiter")
	mode := q.Get("mode")          // "csv" or "json"
	datePref := q.Get("date_pref") // "auto"|"eu"|"us"

	nbytes, _ := strconv.Atoi(bytesStr)
	opt := probe.Options{
		URL:            url,
		MaxBytes:       nbytes,
		Delimiter:      probe.DecodeDelimiter(delimiter),
		Name:           name,
		OutputJSON:     mode == "json",
		DatePreference: datePref,
	}
	res, err := probe.Probe(opt)
	if err != nil {
		http.Error(w, "probe failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	// CSV vs JSON — both fine as text/plain so browsers render simply.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(res.Body)
}

// indexHTML is an embedded, minimal page with Tailwind-less vanilla styling.
//
//go:embed index.tmpl.html
var indexHTML string
