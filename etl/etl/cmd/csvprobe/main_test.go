package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const helperEnv = "GO_WANT_MAIN_HELPER"

// TestHelperProcess is a standard sub-process test helper.
// When invoked with GO_WANT_MAIN_HELPER=1, it will:
//  1. Strip arguments up to and including a literal "--" marker
//  2. Set os.Args to the remaining list (your CLI flags)
//  3. Call main()
//  4. Exit(0) on success
//
// Parent tests run this as: test-binary -test.run=TestHelperProcess -- <flags...>
// (See each test below for details.)
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		return
	}

	// Extract the CLI args after the conventional "--" separator so main() sees only your flags.
	args := os.Args
	sep := -1
	for i, a := range args {
		if a == "--" {
			sep = i
			break
		}
	}
	if sep >= 0 && sep+1 < len(args) {
		os.Args = append([]string{args[0]}, args[sep+1:]...)
	} else {
		// No flags passed; just run main with defaults.
		os.Args = []string{args[0]}
	}

	// Run your CLI.
	main()
	// If main() panics, this helper process exits nonzero automatically.
	os.Exit(0)
}

// runMainSubprocess runs the test binary in a separate process,
// invoking TestHelperProcess which calls main() with the provided flags.
func runMainSubprocess(t *testing.T, workdir string, flags ...string) (stdout string, stderr string, err error) {
	t.Helper()

	// Build the command: current test binary, tell it to run the helper test function,
	// and pass your CLI flags after a "--" separator.
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess", "--")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	cmd.Args = append(cmd.Args, flags...)

	if workdir != "" {
		cmd.Dir = workdir
	}

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

// makeTestServer returns an httptest.Server that serves the given body with 200.
func makeTestServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a normal CSV response with correct headers (not required but realistic).
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		io.WriteString(w, body)
	})
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}

// jsonValid is a tiny helper to validate JSON output.
func jsonValid(s string) bool { return json.Valid([]byte(s)) }

// ---------- Tests ----------

// TestMain_JSONOutput verifies that running the CLI with -json against a small,
// well-formed CSV produces valid JSON and includes key fields we expect from the
// probe’s builder: parser comma, header_map, storage, transform.coerce, etc.
func TestMain_JSONOutput(t *testing.T) {
	// A small CSV with Czech headers and DMY dates to exercise type inference and layout scoring.
	// Fields: PČV(int), Typ(text), Stav(text), Kód STK(int), Název STK(text),
	//         Platnost od(date DMY), Platnost do(date DMY), Číslo protokolu(text), Aktuální(bool)
	csv := "" +
		"PČV,Typ,Stav,Kód STK,Název STK,Platnost od,Platnost do,Číslo protokolu,Aktuální\n" +
		"123,A,ok,15,Some STK,02.01.2024,03.01.2024,ABC,1\n" +
		"456,B,ok,22,Other STK,04.01.2024,05.01.2024,DEF,0\n"

	srv := makeTestServer(t, csv)

	// Temporary working directory to keep any artifacts contained (though -save is not used here).
	workdir := t.TempDir()

	// Run CLI as JSON with explicit delimiter "," and a known name.
	stdout, stderr, err := runMainSubprocess(t, workdir,
		"-url", srv.URL,
		"-bytes", "200000",
		"-delimiter", ",",
		"-name", "try_this",
		"-json",
		"-datepref", "auto",
	)
	if err != nil {
		t.Fatalf("main returned error: %v, stderr: %s", err, stderr)
	}
	if !jsonValid(stdout) {
		t.Fatalf("output is not valid JSON:\n%s", stdout)
	}

	// Basic sanity checks over output content.
	// 1) Parser comma
	if !strings.Contains(stdout, `"comma": ","`) {
		t.Errorf("expected parser comma to be \",\", got:\n%s", stdout)
	}
	// 2) header_map contains a known mapping (diacritics kept in key)
	if !(strings.Contains(stdout, `"PČV": "pcv"`) || strings.Contains(stdout, `"P\u010cV": "pcv"`)) {
		t.Errorf("expected header_map to include PČV→pcv, got:\n%s", stdout)
	}
	// 3) storage table uses normalized name
	if !strings.Contains(stdout, `"table": "public.try_this"`) {
		t.Errorf("expected storage table public.try_this, got:\n%s", stdout)
	}
	// 4) transform includes a coerce step with a layout (DMY)
	if !strings.Contains(stdout, `"kind": "coerce"`) {
		t.Errorf("expected a coerce transform, got:\n%s", stdout)
	}
	// DMY layout presence
	if !strings.Contains(stdout, `"layout": "02.01.2006"`) {
		t.Errorf("expected coerce.layout 02.01.2006, got:\n%s", stdout)
	}
	// 5) coerce.types should include non-text fields, e.g., pcv (int) and aktualni (bool)
	if !strings.Contains(stdout, `"pcv": "int"`) || !strings.Contains(stdout, `"aktualni": "bool"`) {
		t.Errorf("expected coerce.types to include pcv:int and aktualni:bool, got:\n%s", stdout)
	}
	// 6) auto_create_table present in storage
	if !strings.Contains(stdout, `"auto_create_table": true`) {
		t.Errorf("expected storage.postgres.auto_create_table true, got:\n%s", stdout)
	}
}

// TestMain_CSVOutput verifies the default non-JSON output path (CSV summary lines).
// It ensures the CLI prints "header,normalized,type" lines and that the normalized
// names/types line up with the headers in the sample.
func TestMain_CSVOutput(t *testing.T) {
	csv := "" +
		"PČV,Typ,Stav\n" +
		"123,A,ok\n" +
		"456,B,ok\n"

	srv := makeTestServer(t, csv)
	workdir := t.TempDir()

	stdout, stderr, err := runMainSubprocess(t, workdir,
		"-url", srv.URL,
		"-bytes", "50000",
		"-delimiter", ",",
		"-name", "small_case",
		// -json is omitted → CSV mode
	)
	if err != nil {
		t.Fatalf("main returned error: %v, stderr: %s", err, stderr)
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 lines (headers), got %d:\n%s", len(lines), stdout)
	}
	// First line should be: original header, normalized name, inferred type (integer for PČV)
	if !strings.HasPrefix(lines[0], "PČV,pcv,integer") && !strings.HasPrefix(lines[0], "P\u010cV,pcv,integer") {
		t.Errorf("unexpected first line: %s", lines[0])
	}
	// Second and third lines are text columns
	if !strings.HasSuffix(lines[1], ",typ,text") {
		t.Errorf("unexpected second line: %s", lines[1])
	}
	if !strings.HasSuffix(lines[2], ",stav,text") {
		t.Errorf("unexpected third line: %s", lines[2])
	}
}

// TestMain_SaveWritesFile verifies that using the -save flag causes the sampled
// bytes to be written to [normalize(name)].csv in the working directory. We check
// the file exists and contains the CSV we served (trimmed at the last newline).
func TestMain_SaveWritesFile(t *testing.T) {
	csv := "" +
		"col1;col2\n" +
		"1;2\n" +
		"3;4\n"

	srv := makeTestServer(t, csv)

	// Run in a temp directory to isolate artifacts.
	workdir := t.TempDir()

	// Choose a simple ASCII name to avoid worrying about normalization in the assertion.
	name := "vlastnik_vozidla"

	stdout, stderr, err := runMainSubprocess(t, workdir,
		"-url", srv.URL,
		"-bytes", "100000",
		"-delimiter", ";",
		"-name", name,
		"-save",           // trigger write of [name].csv
		"-json",           // mode doesn't matter for the write; keep JSON to exercise both paths elsewhere
		"-datepref", "eu", // arbitrary
	)
	if err != nil {
		t.Fatalf("main returned error: %v, stderr: %s\nstdout:\n%s", err, stderr, stdout)
	}

	// The file should be named [name].csv in the working directory.
	path := filepath.Join(workdir, name+".csv")
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("expected sample file at %s to exist: %v", path, readErr)
	}

	// The saved content should include our served CSV and end at the last newline.
	got := string(data)
	if !strings.Contains(got, "col1;col2\n1;2\n3;4\n") {
		t.Errorf("unexpected saved content:\n%s", got)
	}
}

// TestMain_DelimiterSemicolon verifies that the delimiter flag is respected in JSON output.
// Specifically, we set -delimiter=";" and expect "comma": ";" in the JSON.
func TestMain_DelimiterSemicolon(t *testing.T) {
	csv := "A;B;C\n1;2;3\n"
	// Note: The underlying CSV reader will use the supplied delimiter; for this test,
	// we only assert that the JSON contains the chosen delimiter in parser.options.comma.
	srv := makeTestServer(t, csv)
	workdir := t.TempDir()

	stdout, stderr, err := runMainSubprocess(t, workdir,
		"-url", srv.URL,
		"-bytes", "4096",
		"-delimiter", ";",
		"-name", "semi_case",
		"-json",
	)
	if err != nil {
		t.Fatalf("main returned error: %v, stderr: %s", err, stderr)
	}
	if !jsonValid(stdout) {
		t.Fatalf("output is not valid JSON:\n%s", stdout)
	}
	if !strings.Contains(stdout, `"comma": ";"`) {
		t.Errorf("expected parser comma ';', got:\n%s", stdout)
	}
}

// TestMain_PanicOnBadURL verifies that a clearly invalid URL (unreachable port)
// causes the process to fail (panic path in main).
func TestMain_PanicOnBadURL(t *testing.T) {
	// 127.0.0.1:0 is invalid/unreachable; fetch should error and main should panic.
	// We run the sub-process and expect a non-zero exit status.
	workdir := t.TempDir()
	// Windows can't connect to "0" either; this should be cross-platform. If a platform
	// treats it differently, we still expect a connect error from fetchFirstBytes.
	badURL := "http://127.0.0.1:0/bogus.csv"

	_, _, err := runMainSubprocess(t, workdir,
		"-url", badURL,
		"-bytes", "1024",
		"-delimiter", ",",
		"-name", "bad",
	)
	if err == nil {
		t.Fatalf("expected non-zero exit (panic) for bad URL, but got success")
	}
	// Optionally assert the error type (ExitError) and OS-exit status fields.
	if ee, ok := err.(*exec.ExitError); ok {
		_ = ee // ExitError confirms non-zero exit; message content is platform-dependent.
	}
}

// -------- Optional helper for local debugging --------

func TestJustValidateHelper(t *testing.T) {
	if runtime.GOOS == "js" {
		t.Skip("skip on gopherjs/wasm")
	}
	_ = time.Now() // silence "imported and not used" if debugging locally with manual tweaks
}
