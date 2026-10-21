package config

// ultra_config_test.go fills the UNCOVERED config-layer gaps from
// doc/ultra_test.md §1: CFG-2, CFG-4, CFG-5, CFG-7, CFG-11, CFG-13.
//
// These assert the ACTUAL observable behavior of config.Load / RegisterFlags /
// bindFlagsTo and cfgtree.Tree, not "doesn't crash". Helpers writeFile /
// daoCfg / srcCfg / roleCfg / logCfg live in loader_test.go / config_test.go in
// this same package and are reused here.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/aura-studio/tango/internal/cfgtree"
)

// ---------------------------------------------------------------------------
// CFG-2: only USER-SET flags override (flags.Visit semantics). An UNSET flag
// must NOT clobber a value coming from the config file / env with its empty
// default. bindFlagsTo uses flags.Visit, which only walks flags the user
// explicitly set.
// ---------------------------------------------------------------------------

func TestUltraCFG2_UnsetFlagDoesNotClobberFile(t *testing.T) {
	flags := pflag.NewFlagSet("ultra", pflag.ContinueOnError)
	RegisterFlags(flags)

	// User sets ONLY one flag (logging.level). dao.mongo.uri and logging.format
	// are left at their empty "" defaults and must NOT win over the file.
	if err := flags.Parse([]string{"--logging.level", "warn"}); err != nil {
		t.Fatal(err)
	}

	tree, err := Load(writeFile(t, "tango.yaml", `
logging:
  level: error
  format: json
dao:
  mongo:
    uri: "mongodb://localhost/keepme"
`), flags)
	if err != nil {
		t.Fatal(err)
	}

	lc := logCfg(t, tree)
	// The flag the user SET wins over the file.
	if lc.Level != "warn" {
		t.Errorf("logging.level = %q, want warn (the user-set flag must win)", lc.Level)
	}
	// The flag the user did NOT set must not push its empty default; the file
	// value survives.
	if lc.Format != "json" {
		t.Errorf("logging.format = %q, want json (unset --logging.format must NOT clobber file)", lc.Format)
	}
	// Likewise dao.mongo.uri: unset flag must not blank it (an empty URI would
	// fail dao.Validate, so daoCfg itself would have fatally errored — assert
	// the value explicitly too).
	if got := daoCfg(t, tree).Mongo.URI; got != "mongodb://localhost/keepme" {
		t.Errorf("dao.mongo.uri = %q, want mongodb://localhost/keepme (unset flag must not clobber)", got)
	}
}

// CFG-2 (env variant): an unset flag must not clobber an env-supplied value
// either. The unset flag's empty default loses to TANGO_* env.
func TestUltraCFG2_UnsetFlagDoesNotClobberEnv(t *testing.T) {
	os.Setenv("TANGO_LOGGING_FORMAT", "json")
	defer os.Unsetenv("TANGO_LOGGING_FORMAT")

	flags := pflag.NewFlagSet("ultra", pflag.ContinueOnError)
	RegisterFlags(flags)
	if err := flags.Parse([]string{"--logging.level", "debug"}); err != nil {
		t.Fatal(err)
	}

	tree, err := Load(writeFile(t, "tango.yaml", `
dao:
  mongo:
    uri: "mongodb://localhost/x"
`), flags)
	if err != nil {
		t.Fatal(err)
	}
	lc := logCfg(t, tree)
	if lc.Level != "debug" {
		t.Errorf("logging.level = %q, want debug (set flag wins)", lc.Level)
	}
	if lc.Format != "json" {
		t.Errorf("logging.format = %q, want json (unset --logging.format must NOT clobber env)", lc.Format)
	}
}

// ---------------------------------------------------------------------------
// CFG-4: --config is a file PATH, not a config key. RegisterFlags enumerates
// only registerAll's keys (none of which is "config"), so it does NOT define a
// "config" flag; and bindFlagsTo explicitly skips a flag named "config" so a
// --config the root command owns never becomes the "config" viper key.
// ---------------------------------------------------------------------------

func TestUltraCFG4_RegisterFlagsDoesNotCreateConfigKey(t *testing.T) {
	flags := pflag.NewFlagSet("ultra", pflag.ContinueOnError)
	RegisterFlags(flags)
	if f := flags.Lookup("config"); f != nil {
		t.Errorf("RegisterFlags must NOT register a \"config\" key flag; got %+v", f)
	}
	// Sanity: real config keys ARE registered, proving the flag set is populated.
	if flags.Lookup("dao.mongo.uri") == nil {
		t.Fatal("expected --dao.mongo.uri to be registered")
	}
}

// CFG-4: even if the root command added a --config file-path flag and the user
// set it, bindFlagsTo skips it so it never becomes the "config" key. We model
// the root command's --config by registering it ourselves, set it to a path,
// and assert Load ignores it (treats it as a path, not a key): the load still
// succeeds and no "config" leaf is observable.
func TestUltraCFG4_BindFlagsSkipsConfigFlag(t *testing.T) {
	flags := pflag.NewFlagSet("ultra", pflag.ContinueOnError)
	RegisterFlags(flags)
	// The root command owns --config separately (a string file path).
	flags.String("config", "", "config file path")
	if err := flags.Parse([]string{"--config", "/some/path/that/is/a/file"}); err != nil {
		t.Fatal(err)
	}

	tree, err := Load(writeFile(t, "tango.yaml", `
dao:
  mongo:
    uri: "mongodb://localhost/x"
`), flags)
	if err != nil {
		t.Fatalf("Load must ignore the --config flag value (it is a path, not a key): %v", err)
	}
	// The "config" flag value must NOT have leaked into the tree as a key. Decode
	// the whole tree to a map and assert there is no top-level "config" branch
	// equal to the file path.
	var whole map[string]any
	if err := tree.Into(&whole); err != nil {
		t.Fatalf("decode whole tree: %v", err)
	}
	if v, ok := whole["config"]; ok {
		t.Errorf("--config leaked into tree as key \"config\" = %v; it must be skipped", v)
	}
	// And the real config still loaded.
	if got := daoCfg(t, tree).Mongo.URI; got != "mongodb://localhost/x" {
		t.Errorf("dao.mongo.uri = %q, want mongodb://localhost/x", got)
	}
}

// ---------------------------------------------------------------------------
// CFG-5: every registered config key has a matching --<key> flag with the
// IDENTICAL dotted name (case preserved). Enumerate the known keys across all
// modules and assert each flag exists.
// ---------------------------------------------------------------------------

func TestUltraCFG5_EveryKnownKeyHasFlag(t *testing.T) {
	flags := pflag.NewFlagSet("ultra", pflag.ContinueOnError)
	RegisterFlags(flags)

	wantKeys := []string{
		// logging
		"logging.level",
		"logging.format",
		// dao
		"dao.mongo.uri",
		"dao.mongo.connectTimeout",
		"dao.mongo.serverSelectionTimeout",
		"dao.store.maxElapsedTime",
		// parser
		"parser.filter.include",
		"parser.filter.exclude",
		// source (tailer) — note mixed-case logPattern / tailMode are preserved
		"source.tailer.logPattern",
		"source.tailer.tailMode",
		"source.tailer.rescanInterval",
		"source.tailer.pollInterval",
		"source.tailer.maxLineBytes",
		"source.tailer.maxOpenFDs",
		// process
		"process.mode",
		"process.batchSize",
		"process.pipeline.batchWorkers",
		"process.pipeline.flushInterval",
		// cfgsync — note there is deliberately NO cfgsync.collection here: the
		// central config collection became the hardcoded cfgsync.DefaultCollection
		// constant and the key was removed outright (178245e), so an old config
		// still setting it is silently ignored as an unknown key. See the NOTE in
		// internal/cfgsync/config.go. Do not re-add it.
		"cfgsync.enabled",
		"cfgsync.backend",
		"cfgsync.documentID",
		// role
		"role.mode",
		"role.gateway.addr",
		"role.cli.function",
	}
	for _, k := range wantKeys {
		if f := flags.Lookup(k); f == nil {
			t.Errorf("expected --%s flag registered, but it is missing", k)
		} else if f.Value.Type() != "string" {
			// RegisterFlags defines every key flag as a string (overrides via text).
			t.Errorf("flag --%s type = %q, want string", k, f.Value.Type())
		}
	}

	// The exact-case names must be present (case is preserved, not lowercased):
	// a lowercased variant must NOT be how it is registered for camelCase keys.
	if flags.Lookup("source.tailer.logpattern") != nil {
		t.Error("flag should be registered as mixed-case source.tailer.logPattern, not all-lowercase")
	}
}

// ---------------------------------------------------------------------------
// CFG-7: a path that EXISTS but is a directory / malformed YAML content makes
// Load return a wrapped error whose message mentions the file/parse; an empty
// or nonexistent path returns no error (defaults only).
// ---------------------------------------------------------------------------

func TestUltraCFG7_DirectoryPathReturnsError(t *testing.T) {
	dir := t.TempDir() // exists, stat succeeds, but it is a directory
	_, err := Load(dir, nil)
	if err == nil {
		t.Fatalf("Load(<directory>) = nil error, want a wrapped read error")
	}
	if !strings.Contains(err.Error(), "read config") {
		t.Errorf("error %q should be wrapped with \"read config\"", err.Error())
	}
	if !strings.Contains(err.Error(), dir) {
		t.Errorf("error %q should mention the offending path %q", err.Error(), dir)
	}
}

func TestUltraCFG7_MalformedYAMLReturnsError(t *testing.T) {
	// Invalid YAML (a bare unterminated bracket) under a .yaml extension forces
	// the YAML parser and fails inside ReadInConfig.
	p := writeFile(t, "bad.yaml", "dao:\n  mongo:\n    uri: [unterminated\n")
	_, err := Load(p, nil)
	if err == nil {
		t.Fatalf("Load(<malformed yaml>) = nil error, want a wrapped parse error")
	}
	if !strings.Contains(err.Error(), "read config") {
		t.Errorf("error %q should be wrapped with \"read config\"", err.Error())
	}
	if !strings.Contains(err.Error(), p) {
		t.Errorf("error %q should mention the file path %q", err.Error(), p)
	}
}

func TestUltraCFG7_NonexistentPathNoError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")
	tree, err := Load(missing, nil)
	if err != nil {
		t.Fatalf("Load(<nonexistent>) = %v, want nil (silently skipped, defaults apply)", err)
	}
	// Defaults still apply: process.mode defaults to batch.
	if got := procCfg(t, tree).Mode; got != "batch" {
		t.Errorf("process.mode default = %q, want batch", got)
	}
}

func TestUltraCFG7_EmptyPathNoError(t *testing.T) {
	tree, err := Load("", nil)
	if err != nil {
		t.Fatalf("Load(\"\") = %v, want nil (empty path skipped)", err)
	}
	if got := roleCfg(t, tree).Mode; got != "daemon" {
		t.Errorf("role.mode default = %q, want daemon", got)
	}
}

// ---------------------------------------------------------------------------
// CFG-11: cfgtree.Sub().Into() is a no-op when a mid-path segment is NOT a map
// (resolve returns nil), and when a branch is missing entirely — leaving dst at
// its pre-call value so the caller's ApplyDefaults still works.
// ---------------------------------------------------------------------------

type cfg11Dst struct {
	Mode string `mapstructure:"mode"`
}

func TestUltraCFG11_MidPathNotAMapIsNoOp(t *testing.T) {
	// "role" is a STRING, not a map, so Sub("role").Sub("mode") cannot resolve.
	tr := cfgtree.New(map[string]any{"role": "iAmAStringNotAMap"})

	dst := cfg11Dst{Mode: "sentinel"}
	if err := tr.Sub("role").Sub("mode").Into(&dst); err != nil {
		t.Fatalf("Into over a non-map mid-path should be a silent no-op, got err: %v", err)
	}
	if dst.Mode != "sentinel" {
		t.Errorf("dst.Mode = %q, want sentinel (a non-map mid-path must leave dst untouched)", dst.Mode)
	}

	// Contrast: resolving DIRECTLY TO a non-map leaf (not a mid-path miss) returns
	// that node (non-nil), so Into proceeds to decode and mapstructure reports a
	// type error rather than a silent no-op. The struct dst is left untouched.
	// (This is the documented boundary: only a MISSING branch / nil node is the
	// silent no-op; a present-but-wrong-type node surfaces a decode error.)
	dst2 := cfg11Dst{Mode: "sentinel2"}
	err := tr.Sub("role").Into(&dst2)
	if err == nil {
		t.Fatal("Into over a present string node should return a decode error, got nil")
	}
	if !strings.Contains(err.Error(), "expected a map or struct") {
		t.Errorf("error = %q, want mapstructure type error about expecting a map/struct", err.Error())
	}
	if dst2.Mode != "sentinel2" {
		t.Errorf("dst2.Mode = %q, want sentinel2 (failed decode must not partially mutate dst)", dst2.Mode)
	}
}

func TestUltraCFG11_MissingBranchLeavesDstUntouched(t *testing.T) {
	tr := cfgtree.New(map[string]any{"dao": map[string]any{"mongo": map[string]any{"uri": "x"}}})

	dst := cfg11Dst{Mode: "sentinel"}
	// "role" branch is absent — resolve walks to a nil map[k] => returns nil.
	if err := tr.Sub("role").Into(&dst); err != nil {
		t.Fatalf("Into over a missing branch should be a no-op, got err: %v", err)
	}
	if dst.Mode != "sentinel" {
		t.Errorf("dst.Mode = %q, want sentinel (missing branch must leave dst untouched)", dst.Mode)
	}
}

func TestUltraCFG11_ZeroTreeIsNoOp(t *testing.T) {
	var tr cfgtree.Tree // zero value: nil settings, nil path
	dst := cfg11Dst{Mode: "sentinel"}
	if err := tr.Into(&dst); err != nil {
		t.Fatalf("zero Tree Into should be a no-op, got err: %v", err)
	}
	if dst.Mode != "sentinel" {
		t.Errorf("dst.Mode = %q, want sentinel (zero Tree decodes nothing)", dst.Mode)
	}
}

// ---------------------------------------------------------------------------
// CFG-13: viper lowercases keys; mapstructure tag matching is case-INSENSITIVE.
// A leaf whose map key case differs from the struct's mapstructure tag still
// decodes. We exercise both the raw cfgtree path (lowercased key vs camelCase
// tag) and the real env path (TANGO_SOURCE_TAILER_TAILMODE decoding into the
// `tailMode` tagged field).
// ---------------------------------------------------------------------------

type cfg13Tailer struct {
	LogPattern []string `mapstructure:"logPattern"`
	TailMode   string   `mapstructure:"tailMode"`
}

func TestUltraCFG13_CaseInsensitiveTagMatchViaCfgtree(t *testing.T) {
	// The map keys are all-lowercase (as viper's AllSettings would emit), but the
	// struct tags are camelCase. Decoding must still populate both fields.
	tr := cfgtree.New(map[string]any{
		"source": map[string]any{
			"tailer": map[string]any{
				"logpattern": []string{"/tmp/a.log", "/tmp/b.log"},
				"tailmode":   "poll",
			},
		},
	})
	var c cfg13Tailer
	if err := tr.Sub("source").Sub("tailer").Into(&c); err != nil {
		t.Fatalf("Into: %v", err)
	}
	if len(c.LogPattern) != 2 || c.LogPattern[0] != "/tmp/a.log" {
		t.Errorf("LogPattern = %v, want [/tmp/a.log /tmp/b.log] (lowercase key must match camelCase tag)", c.LogPattern)
	}
	if c.TailMode != "poll" {
		t.Errorf("TailMode = %q, want poll (lowercase key must match camelCase tag)", c.TailMode)
	}
}

func TestUltraCFG13_CaseInsensitiveEnvDecode(t *testing.T) {
	// Set the env in upper-snake (its natural form). Viper lowercases the key,
	// and the camelCase mapstructure tag still matches case-insensitively, so the
	// value reaches the typed field through the full Load path.
	os.Setenv("TANGO_SOURCE_TAILER_TAILMODE", "poll")
	defer os.Unsetenv("TANGO_SOURCE_TAILER_TAILMODE")

	tree := treeFromYAML(t, `
dao:
  mongo:
    uri: "mongodb://localhost/x"
source:
  tailer:
    logPattern: ["/tmp/x.log"]
    tailMode: hybrid
`)
	if got := srcCfg(t, tree).Tailer.TailMode; got != "poll" {
		t.Errorf("source.tailer.tailMode = %q, want poll (env overrides file AND decodes case-insensitively)", got)
	}
}
