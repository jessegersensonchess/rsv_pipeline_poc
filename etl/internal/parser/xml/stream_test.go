package xmlparser

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseStream_EmptyXML(t *testing.T) {
	cfg := Config{RecordTag: "Rec", Fields: map[string]string{"a": "A"}}
	comp, _ := Compile(cfg)
	xml := `<Root></Root>`
	out := make(chan Record, 4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Test empty XML input
	err := ParseStream(ctx, strings.NewReader(xml), nil, "Rec", comp, Options{
		Workers: 1, Queue: 4, BufSize: 64 << 10,
	}, out)
	if err != nil {
		t.Fatal(err)
	}
	close(out)

	// Check if no records were parsed
	if len(out) != 0 {
		t.Fatalf("expected 0 records, got %d", len(out))
	}
}

// TestParseStream_Streaming tests parsing XML in a streaming manner using io.Reader.
// It uses the 'r' reader to process a stream of XML data and checks if the
// parsed records are correctly emitted to the 'out' channel.
func TestParseStream_Streaming(t *testing.T) {
	cfg := Config{
		RecordTag: "Rec",
		Fields:    map[string]string{"a": "A"},
	}
	comp, _ := Compile(cfg)
	xml := `<Root><Rec><A>1</A></Rec><Rec><A>2</A></Rec></Root>`
	out := make(chan Record, 4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ParseStream(ctx, strings.NewReader(xml), nil, "Rec", comp, Options{
		Workers: 2, Queue: 4, BufSize: 64 << 10,
	}, out); err != nil {
		t.Fatal(err)
	}
	close(out)
	sum := 0
	for r := range out {
		if r["a"] == "1" || r["a"] == "2" {
			sum++
		}
	}
	if sum != 2 {
		t.Fatalf("expected 2 records, got %d", sum)
	}
}

// TestParseStream_ZeroCopy tests parsing XML with ZeroCopy enabled, where the entire
// XML data is passed as a byte slice instead of being streamed from a reader.
func TestParseStream_ZeroCopy(t *testing.T) {
	cfg := Config{RecordTag: "X", Fields: map[string]string{"v": "V"}}
	comp, _ := Compile(cfg)
	xml := `<Root><X><V>k</V></X></Root>`
	out := make(chan Record, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ParseStream(ctx, io.NopCloser(nil), []byte(xml), "X", comp, Options{
		Workers: 1, Queue: 2, ZeroCopy: true,
	}, out); err != nil {
		t.Fatal(err)
	}
	close(out)
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
}

// todo: fix
//// TestParseStream_WithError tests the behavior of ParseStream when an error occurs
//// due to malformed XML input. The test expects the function to return an error
//// when parsing malformed XML (missing closing tag in this case).
//func TestParseStream_WithError(t *testing.T) {
//	cfg := Config{RecordTag: "Rec", Fields: map[string]string{"a": "A"}}
//	comp, _ := Compile(cfg)
//	xml := `<Root><Rec><A>1</A></Rec><Rec><A>2</Root>` // Malformed XML (missing closing tag)
//	out := make(chan Record, 4)
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//
//	// Test that malformed XML triggers an error
//	err := ParseStream(ctx, strings.NewReader(xml), nil, "Rec", comp, Options{
//		Workers: 2, Queue: 4, BufSize: 64 << 10,
//	}, out)
//	if err == nil {
//		t.Fatal("expected error, got nil")
//	}
//}

// todo: fix
//// TestParseStream_CancelContext tests the behavior of ParseStream when the context is canceled.
//// The test ensures that the function correctly handles context cancellation and returns
//// context.Canceled when appropriate.
//func TestParseStream_CancelContext(t *testing.T) {
//	cfg := Config{RecordTag: "Rec", Fields: map[string]string{"a": "A"}}
//	comp, _ := Compile(cfg)
//	xml := `<Root><Rec><A>1</A></Rec><Rec><A>2</A></Root>`
//	out := make(chan Record, 4)
//	ctx, cancel := context.WithCancel(context.Background())
//
//	// Simulate a context cancellation after a short delay to allow workers to start
//	go func() {
//		time.Sleep(100 * time.Millisecond) // Adding delay before cancel to allow workers to begin processing
//		cancel()
//	}()
//	err := ParseStream(ctx, strings.NewReader(xml), nil, "Rec", comp, Options{
//		Workers: 2, Queue: 4, BufSize: 64 << 10,
//	}, out)
//
//	// Ensure the error is context.Canceled, indicating that the function handled the cancellation properly
//	if err != context.Canceled {
//		t.Fatalf("expected context.Canceled, got %v", err)
//	}
//}
