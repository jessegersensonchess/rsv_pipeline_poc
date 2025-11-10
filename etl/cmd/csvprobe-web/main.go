// Command csvprobe-web starts a tiny web UI for the CSV sampler/inspector.
//
// Usage:
//
//	go run ./cmd/csvprobe-web -addr :8080
package main

import (
	"flag"
	"log"

	"csv_header_fetcher/internal/webui"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	srv := webui.NewServer(webui.Config{
		Addr: *addr,
	})
	log.Printf("listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
