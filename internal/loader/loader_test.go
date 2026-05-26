package loader

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadDir_HappyPath asserts the directory loader picks up both
// .yaml and .yml files, extracts the id, and exposes the raw YAML
// verbatim (the contract that lets RSS publish bytes as-is).
func TestLoadDir_HappyPath(t *testing.T) {
	dir := t.TempDir()
	must := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	must("rule_a.yaml", "id: rule_a\nwhen: \"true\"\n")
	must("rule_b.yml", "id: rule_b\nwhen: \"true\"\n")

	files, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("loaded %d files; want 2", len(files))
	}
	ids := map[string]bool{}
	for _, f := range files {
		ids[f.ID] = true
		if !strings.Contains(f.YAML, "id: "+f.ID) {
			t.Errorf("file %s: YAML body does not contain its own id; got %q", f.ID, f.YAML)
		}
	}
	for _, want := range []string{"rule_a", "rule_b"} {
		if !ids[want] {
			t.Errorf("missing id %q; loaded %v", want, ids)
		}
	}
}

// TestLoadDir_RejectsDuplicateID surfaces a hard config error when
// two files declare the same id. RSS's registry is id-keyed, so a
// silent overwrite would let one rule disappear depending on
// filesystem order. The error must name BOTH files so the operator
// knows what to fix.
func TestLoadDir_RejectsDuplicateID(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.yaml", "b.yaml"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("id: same_rule\nwhen: \"true\"\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	_, err := LoadDir(dir)
	if err == nil {
		t.Fatal("LoadDir with duplicate ids: want error, got nil")
	}
	if !strings.Contains(err.Error(), "same_rule") {
		t.Errorf("error must name the duplicated id; got %v", err)
	}
}

// TestLoadFile_MissingID rejects a file that parses as YAML but has
// no id. RSS's registry is id-keyed and can't tolerate empty keys.
func TestLoadFile_MissingID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "noid.yaml")
	if err := os.WriteFile(path, []byte("when: \"true\"\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := LoadFile(path)
	if err == nil {
		t.Fatal("LoadFile with no id: want error, got nil")
	}
	if !strings.Contains(err.Error(), "id") {
		t.Errorf("error must mention the missing field; got %v", err)
	}
}

// TestLoadFile_BadYAML surfaces a parse error rather than silently
// returning an empty File. The wrapping must include the file path
// so operators can find the broken file in a directory of many.
func TestLoadFile_BadYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.yaml")
	if err := os.WriteFile(path, []byte("id: x\nwhen: [unterminated\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := LoadFile(path)
	if err == nil {
		t.Fatal("LoadFile with malformed yaml: want error, got nil")
	}
	if !strings.Contains(err.Error(), "broken.yaml") {
		t.Errorf("error must include the file path; got %v", err)
	}
}

// TestLoadDir_IgnoresNonYAML asserts the glob filter — a stray
// README, .gitignore, or .swp file in the rules directory must not
// be treated as a rule (would fail YAML parse and crash startup).
func TestLoadDir_IgnoresNonYAML(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "rule.yaml"), []byte("id: r\nwhen: \"true\"\n"), 0o600); err != nil {
		t.Fatalf("write rule: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# notes\n"), 0o600); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".rule.yaml.swp"), []byte("garbage"), 0o600); err != nil {
		t.Fatalf("write swp: %v", err)
	}
	files, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(files) != 1 || files[0].ID != "r" {
		t.Errorf("non-yaml siblings leaked into rule set: %+v", files)
	}
}
