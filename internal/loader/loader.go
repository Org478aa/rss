// Package loader reads rule YAML files from disk and surfaces just enough
// structure for the registry's id-keyed storage.
//
// RSS is deliberately *not* the schema authority — LTC's rule loader is.
// We only parse enough of each file to extract the `id` field so the
// registry can key by it and detect duplicates. The full validation
// (expression compile, follower-reference gate, schedule, emit.on enum)
// happens on LTC's side when the rule lands in its registry. If LTC
// rejects a rule, it logs a slog.Warn and skips it; the operator sees the
// failure there.
//
// This split keeps RSS small and avoids extracting `ltc/internal/rule`
// into a shared module just so RSS can re-validate. The cost is one
// hop of indirection in operator workflows — to see why a rule didn't
// load, you read LTC's log, not RSS's. Acceptable for now.
package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// File is one YAML file on disk: its path and the raw bytes. Caller uses
// the ID to register it; the raw YAML goes on the wire verbatim.
type File struct {
	Path string
	ID   string
	YAML string
}

// LoadDir scans the directory for *.yaml and *.yml files. Returns one
// File per parseable rule. Files that fail the lightweight YAML parse or
// lack an `id` field produce a wrapped error naming the path — RSS treats
// startup load failures as fatal so a syntactically-broken fixture
// cannot silently disappear from the registry.
//
// Order is filesystem-order; the registry sorts on emission so the wire
// output is deterministic regardless.
func LoadDir(dir string) ([]File, error) {
	patterns := []string{"*.yaml", "*.yml"}
	var paths []string
	for _, pat := range patterns {
		m, err := filepath.Glob(filepath.Join(dir, pat))
		if err != nil {
			return nil, fmt.Errorf("glob %s/%s: %w", dir, pat, err)
		}
		paths = append(paths, m...)
	}

	files := make([]File, 0, len(paths))
	seen := make(map[string]string, len(paths))
	for _, p := range paths {
		f, err := LoadFile(p)
		if err != nil {
			return nil, err
		}
		if other, dup := seen[f.ID]; dup {
			return nil, fmt.Errorf("duplicate rule id %q: %s and %s", f.ID, other, p)
		}
		seen[f.ID] = p
		files = append(files, f)
	}
	return files, nil
}

// LoadFile parses one file, returning the id+raw-YAML pair. Errors are
// wrapped with the path so the operator's log line points at the file
// that needs editing.
func LoadFile(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, fmt.Errorf("read %s: %w", path, err)
	}
	id, err := extractID(data)
	if err != nil {
		return File{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return File{Path: path, ID: id, YAML: string(data)}, nil
}

// extractID is intentionally narrow: yaml.Unmarshal into a struct with
// only the `id` field, so we don't take an opinion on any other field
// that the rule schema may grow. Empty id is rejected — the registry's
// id-keyed storage cannot tolerate it.
func extractID(data []byte) (string, error) {
	var meta struct {
		ID string `yaml:"id"`
	}
	if err := yaml.Unmarshal(data, &meta); err != nil {
		return "", err
	}
	if strings.TrimSpace(meta.ID) == "" {
		return "", fmt.Errorf("rule file is missing 'id' field")
	}
	return meta.ID, nil
}
