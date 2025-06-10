package purego

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/ebitengine/purego"
)

// getDuckDBLibrary returns the appropriate library name for the current platform
func getDuckDBLibrary() string {
	switch runtime.GOOS {
	case "darwin":
		return "libduckdb.dylib"
	case "windows":
		return "duckdb.dll"
	default: // linux, *bsd, etc
		return "libduckdb.so"
	}
}

// Library represents a loaded DuckDB shared library
type Library struct {
	handle uintptr
}

// LoadLibrary loads the DuckDB shared library
func LoadLibrary() (*Library, error) {
	// Try multiple locations for the library
	libNames := []string{
		getDuckDBLibrary(), // System default
	}
	
	// Check environment variable for custom location (e.g., from Nix)
	if libDir := os.Getenv("DUCKDB_LIB_DIR"); libDir != "" {
		libNames = append([]string{
			filepath.Join(libDir, getDuckDBLibrary()),
		}, libNames...)
	}
	
	// Try loading from each location
	var lastErr error
	for _, libName := range libNames {
		handle, err := purego.Dlopen(libName, purego.RTLD_NOW|purego.RTLD_GLOBAL)
		if err == nil {
			return &Library{handle: handle}, nil
		}
		lastErr = err
	}
	
	return nil, fmt.Errorf("failed to load DuckDB library from any location: %w", lastErr)
}

// Close closes the loaded library
func (l *Library) Close() error {
	if l.handle != 0 {
		return purego.Dlclose(l.handle)
	}
	return nil
}

// RegisterFunc registers a function from the library
func (l *Library) RegisterFunc(fn interface{}, name string) error {
	return purego.RegisterLibFunc(fn, l.handle, name)
}