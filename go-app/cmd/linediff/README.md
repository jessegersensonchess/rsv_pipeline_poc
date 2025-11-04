# linediff

`linediff` is a high-throughput diff helper for **very large line-based files** (CSV, logs, TSV, JSONL, etc).  
It prints every line from **file2** that is **not present** in **file1**, preserving the header so that downstream tools can ingest the output.

It is designed for **speed, streaming, and low memory usage**:
- Processes tens of millions of lines in seconds
- Streams input — does **not** load full files into memory
- Parallel scanning across CPU cores
- Cache-friendly, compact in-memory index of file1 lines
- Optimized hashing (xxh3 or xxhash64)
- CRLF-aware; handles extremely long lines
- Works with large files (`>10 GB`) on standard hardware

---

## Example

```sh
linediff -file1 old.csv -file2 new.csv > diff.csv

linediff -flush=262144 -workers 4 -block=262144 -file1 RSV_vypis_vozidel_20250902.csv -file2 RSV_vypis_vozidel_20251002.csv

## Build
build a windows exe with:
GOOS=windows GOARCH=amd64 go build -o linediff.exe cmd/linediff/main.go
  
docker compose build linediff  
docker compose run --rm linediff -file1 vypis-a.csv -file2 vypis-b.csv

