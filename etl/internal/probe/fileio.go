package probe

import (
	"os"
)

// writeSampleCSV writes the sampled bytes to the specified path.
// It overwrites the file if it already exists.
func writeSampleCSV(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}
