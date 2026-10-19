// Package config reads the file --config names.
//
// §3 standardises --config and --context, and §9 shows the shape it expects:
// `stellar-ingest run --config ingest.yaml`. The problem it solves here is
// concrete -- a mainnet dispatch passes about twenty flags, the same twenty
// every time, and the only place they were written down was a Nomad job
// file. A context is those twenty flags under a name.
//
// A context sets DEFAULTS, never overrides. An explicit flag always wins,
// because the alternative is a file silently changing what a command line
// plainly says. That precedence is the whole contract:
//
//	explicit flag  >  context in --config  >  the flag's built-in default
//
// Credentials are deliberately absent from this file format. §5 keeps
// secrets out of arguments; putting them in a config file instead just moves
// the problem into a file people commit. They stay in the environment.
package config

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// File is a parsed config file.
type File struct {
	// DefaultContext names the context to use when --context is not given.
	DefaultContext string `yaml:"default-context"`
	// Contexts maps a name to that context's flag values. Values are
	// strings so one parser -- the flag table's -- handles every type, and
	// so the file cannot disagree with the flag about what "8" means.
	Contexts map[string]map[string]string `yaml:"contexts"`
}

// Load reads and parses a config file.
func Load(path string) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var file File
	if err := yaml.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if len(file.Contexts) == 0 {
		return nil, fmt.Errorf("config %s defines no contexts", path)
	}
	return &file, nil
}

// ContextNames lists the defined contexts, sorted, for error messages.
func (f *File) ContextNames() []string {
	names := make([]string, 0, len(f.Contexts))
	for name := range f.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Select returns the named context's values.
//
// An empty name takes default-context; a file with several contexts and no
// default is an error rather than a guess, because guessing which network
// to ingest is not a guess anybody wants made for them.
func (f *File) Select(name string) (string, map[string]string, error) {
	if name == "" {
		name = f.DefaultContext
	}
	if name == "" {
		if len(f.Contexts) == 1 {
			// One context and no default is unambiguous.
			for only, values := range f.Contexts {
				return only, values, nil
			}
		}
		return "", nil, fmt.Errorf(
			"no --context given and the config sets no default-context; defined contexts are %s",
			strings.Join(f.ContextNames(), ", "))
	}
	values, ok := f.Contexts[name]
	if !ok {
		return "", nil, fmt.Errorf("config has no context %q; defined contexts are %s",
			name, strings.Join(f.ContextNames(), ", "))
	}
	return name, values, nil
}
