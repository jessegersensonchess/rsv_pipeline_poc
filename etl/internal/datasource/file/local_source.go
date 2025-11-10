package file

import (
	"context"
	"fmt"
	"io" // <-- add this
	"os"
)

type Local struct{ path string }

func NewLocal(path string) *Local { return &Local{path: path} }

func (l *Local) Open(ctx context.Context) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	f, err := os.Open(l.path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", l.path, err)
	}
	return f, nil
}
