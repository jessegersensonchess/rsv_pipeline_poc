// fetchcsv_test.go contains tests for the fetchCSVData helper, which is
// responsible for retrieving CSV data from a remote HTTP endpoint.
//
// The tests use net/http/httptest to simulate HTTP servers with different
// behaviors, allowing the fetchCSVData logic to be verified without making
// real network calls.
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

/*
TestFetchCSVDataOK verifies that fetchCSVData successfully returns a reader
when the remote server responds with an HTTP 200 status code and a valid body.

The test uses an httptest.Server to serve a single CSV record and then
checks that the reader returned by fetchCSVData yields exactly the same
bytes as were written by the test server.
*/
func TestFetchCSVDataOK(t *testing.T) {
	t.Parallel()

	const body = "100,a,b,c,200\n"

	// Set up a test server that always returns HTTP 200 with a known payload.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, body)
	}))
	defer srv.Close()

	rc, err := fetchCSVData(srv.URL)
	if err != nil {
		t.Fatalf("fetchCSVData(%q) error: %v", srv.URL, err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("io.ReadAll: %v", err)
	}

	if string(got) != body {
		t.Fatalf("unexpected body: got %q, want %q", string(got), body)
	}
}

/*
TestFetchCSVDataBadStatus verifies that fetchCSVData returns an error when the
remote server responds with a non-OK HTTP status code.

The test uses an httptest.Server that responds with HTTP 400. The returned
error is checked to ensure it contains the expected prefix indicating a
failed fetch.
*/
func TestFetchCSVDataBadStatus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	rc, err := fetchCSVData(srv.URL)
	if err == nil {
		// The function should fail for non-200 responses and must not return
		// a usable reader in this case.
		if rc != nil {
			t.Logf("fetchCSVData returned non-nil reader despite error")
			rc.Close()
		}
		t.Fatal("expected error for non-200 status, got nil")
	}

	if !strings.Contains(err.Error(), "failed to fetch data") {
		t.Fatalf("unexpected error text: %v", err)
	}
}
