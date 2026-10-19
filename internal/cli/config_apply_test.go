package cli

// The precedence rule is the contract:
//
//	explicit flag  >  context in --config  >  the flag's built-in default
//
// A context that could override a typed flag is a file silently changing
// what a command line plainly says, which is the behaviour that makes
// people stop trusting config files.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/withObsrvr/stellar-ingest/internal/exit"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ingest.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const twoContexts = `
default-context: mainnet
contexts:
  mainnet:
    network: mainnet
    storage-type: GCS
    bucket: obsrvr-mainnet/landing/ledgers
    workers: "12"
    ducklake-metadata-schema: bronze_meta
    chunk-size: "10000"
  testnet:
    network: testnet
    storage-type: GCS
    bucket: obsrvr-testnet/landing/ledgers
    workers: "4"
`

func nothingSet(string) bool { return false }

func TestContextSuppliesDefaults(t *testing.T) {
	v := &Values{ConfigPath: writeConfig(t, twoContexts)}
	if _, err := ApplyConfig(v, nothingSet); err != nil {
		t.Fatalf("apply: %v", err)
	}

	if v.StorageType != "GCS" {
		t.Errorf("storage-type = %q, want GCS", v.StorageType)
	}
	if v.Bucket != "obsrvr-mainnet/landing/ledgers" {
		t.Errorf("bucket = %q", v.Bucket)
	}
	if v.Workers != 12 {
		t.Errorf("workers = %d, want 12 (an int parsed from the file)", v.Workers)
	}
	if v.ChunkSize != 10000 {
		t.Errorf("chunk-size = %d, want 10000", v.ChunkSize)
	}
	if v.NetworkPassphrase != Networks["mainnet"] {
		t.Errorf("passphrase = %q, want the mainnet passphrase", v.NetworkPassphrase)
	}
}

// The one rule that must never bend.
func TestExplicitFlagsBeatTheContext(t *testing.T) {
	v := &Values{ConfigPath: writeConfig(t, twoContexts), Workers: 2, Bucket: "mine"}
	given := map[string]bool{"workers": true, "bucket": true}

	notices, err := ApplyConfig(v, func(name string) bool { return given[name] })
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if v.Workers != 2 {
		t.Errorf("workers = %d; the context overwrote a flag the caller typed", v.Workers)
	}
	if v.Bucket != "mine" {
		t.Errorf("bucket = %q; the context overwrote a flag the caller typed", v.Bucket)
	}
	// And it must say so, or somebody debugs a config file that is being
	// correctly ignored.
	joined := strings.Join(notices, "\n")
	if !strings.Contains(joined, "--workers") || !strings.Contains(joined, "ignored") {
		t.Errorf("no notice that --workers overrode the context:\n%s", joined)
	}
}

func TestContextSelection(t *testing.T) {
	v := &Values{ConfigPath: writeConfig(t, twoContexts), Context: "testnet"}
	if _, err := ApplyConfig(v, nothingSet); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if v.Workers != 4 {
		t.Errorf("workers = %d, want testnet's 4", v.Workers)
	}
	if v.NetworkPassphrase != Networks["testnet"] {
		t.Errorf("passphrase is not testnet's: %q", v.NetworkPassphrase)
	}
}

// A typo in a config file must not be a setting that silently does nothing,
// which is how a bucket stays pointed at the wrong network.
func TestUnknownOptionIsRejected(t *testing.T) {
	path := writeConfig(t, "contexts:\n  a:\n    wokers: \"8\"\n")
	_, err := ApplyConfig(&Values{ConfigPath: path}, nothingSet)
	if err == nil {
		t.Fatal("unknown option accepted; a typo would silently do nothing")
	}
	if !strings.Contains(err.Error(), "wokers") {
		t.Errorf("error does not name the offending key: %v", err)
	}
	if got := exit.ClassOf(err); got != exit.Usage {
		t.Errorf("exit class = %v, want %v", got, exit.Usage)
	}
}

func TestUnknownContextIsRejectedAndListsWhatExists(t *testing.T) {
	v := &Values{ConfigPath: writeConfig(t, twoContexts), Context: "mainnnet"}
	_, err := ApplyConfig(v, nothingSet)
	if err == nil {
		t.Fatal("unknown context accepted")
	}
	for _, want := range []string{"mainnnet", "mainnet", "testnet"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
}

// Guessing which network to ingest is not a guess anybody wants made for
// them.
func TestAmbiguousContextIsAnError(t *testing.T) {
	path := writeConfig(t, "contexts:\n  a:\n    workers: \"1\"\n  b:\n    workers: \"2\"\n")
	_, err := ApplyConfig(&Values{ConfigPath: path}, nothingSet)
	if err == nil {
		t.Fatal("two contexts and no default was accepted; the tool guessed which network to use")
	}
}

// One context and no default is unambiguous, so it should just work.
func TestSingleContextNeedsNoDefault(t *testing.T) {
	path := writeConfig(t, "contexts:\n  only:\n    workers: \"7\"\n")
	v := &Values{ConfigPath: path}
	if _, err := ApplyConfig(v, nothingSet); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if v.Workers != 7 {
		t.Errorf("workers = %d, want 7", v.Workers)
	}
}

func TestContextWithoutConfigIsRejected(t *testing.T) {
	_, err := ApplyConfig(&Values{Context: "mainnet"}, nothingSet)
	if err == nil {
		t.Fatal("--context without --config was accepted")
	}
}

func TestNetworkShortNames(t *testing.T) {
	for name, want := range Networks {
		v := &Values{Network: name}
		if _, err := ApplyConfig(v, nothingSet); err != nil {
			t.Fatalf("--network %s: %v", name, err)
		}
		if v.NetworkPassphrase != want {
			t.Errorf("--network %s gave passphrase %q, want %q", name, v.NetworkPassphrase, want)
		}
	}
}

// Typing a passphrase by hand is how a testnet run ends up labelled
// mainnet: the two strings differ by one word in the middle. An unknown
// short name must fail rather than silently leave the default.
func TestUnknownNetworkIsRejected(t *testing.T) {
	_, err := ApplyConfig(&Values{Network: "mainet"}, nothingSet)
	if err == nil {
		t.Fatal("--network mainet accepted; the run would have used the default passphrase")
	}
	if !strings.Contains(err.Error(), "mainnet") {
		t.Errorf("error does not suggest the right spelling: %v", err)
	}
}

// The long form stays authoritative, so a network this table does not know
// is still reachable.
func TestExplicitPassphraseBeatsNetworkShortName(t *testing.T) {
	v := &Values{Network: "mainnet", NetworkPassphrase: "Standalone Network ; February 2017"}
	given := map[string]bool{"network-passphrase": true}
	if _, err := ApplyConfig(v, func(n string) bool { return given[n] }); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if v.NetworkPassphrase != "Standalone Network ; February 2017" {
		t.Errorf("passphrase = %q; --network overrode an explicit --network-passphrase", v.NetworkPassphrase)
	}
}

func TestNoConfigIsNotAnError(t *testing.T) {
	v := &Values{}
	notices, err := ApplyConfig(v, nothingSet)
	if err != nil {
		t.Fatalf("no config should be fine: %v", err)
	}
	if len(notices) != 0 {
		t.Errorf("no config produced notices: %v", notices)
	}
}

func TestMissingConfigFileIsAUsageError(t *testing.T) {
	_, err := ApplyConfig(&Values{ConfigPath: "/nonexistent/ingest.yaml"}, nothingSet)
	if err == nil {
		t.Fatal("missing config accepted")
	}
	if got := exit.ClassOf(err); got != exit.Usage {
		t.Errorf("exit class = %v, want %v", got, exit.Usage)
	}
}

// Every type the flag table knows must round-trip from the file, or a
// context can set some options and silently not others.
func TestEveryFlagKindCanComeFromAContext(t *testing.T) {
	path := writeConfig(t, `
contexts:
  all:
    bucket: a-string
    ducklake: "true"
    chunk-size: "500"
    workers: "3"
    timeout: 90m
`)
	v := &Values{ConfigPath: path}
	if _, err := ApplyConfig(v, nothingSet); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if v.Bucket != "a-string" {
		t.Errorf("string: %q", v.Bucket)
	}
	if !v.RunDuckLake {
		t.Error("bool did not apply")
	}
	if v.ChunkSize != 500 {
		t.Errorf("uint: %d", v.ChunkSize)
	}
	if v.Workers != 3 {
		t.Errorf("int: %d", v.Workers)
	}
	if v.Timeout.Minutes() != 90 {
		t.Errorf("duration: %v", v.Timeout)
	}
}

func TestBadValueNamesTheOption(t *testing.T) {
	path := writeConfig(t, "contexts:\n  a:\n    workers: not-a-number\n")
	_, err := ApplyConfig(&Values{ConfigPath: path}, nothingSet)
	if err == nil {
		t.Fatal("non-numeric workers accepted")
	}
	if !strings.Contains(err.Error(), "workers") {
		t.Errorf("error does not name the option: %v", err)
	}
}
