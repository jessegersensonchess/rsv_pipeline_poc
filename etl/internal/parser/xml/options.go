package xmlparser

// Options controls performance behavior of the XML parser.
// All fields are optional; zero values pick sensible defaults.
type Options struct {
	// Concurrency & channels
	Workers int // number of worker goroutines; 0 => 1
	Queue   int // channel capacity; 0 => 4*Workers (or 4 when Workers==0)

	// Sharding / buffering
	BufSize  int  // bufio.Reader size for non-zerocopy; 0 => 1<<20
	ZeroCopy bool // use zero-copy sharding (requires whole bytes buffer)

	// Fast-paths
	UltraFast bool // enable schema-like raw byte extractor
	Schema    bool // alias for schema-driven fast path; treated same as UltraFast

	// Optional output ordering (default: unordered for throughput)
	PreserveOrder bool // when true, preserve exact input order
	OrderWindow   int  // bounded reordering window; 0 => unordered

	// Debug mode
	Debug bool
}
