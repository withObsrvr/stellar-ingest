package cli

// Applying a config context, and resolving --network.
//
// The precedence rule is the whole contract and it lives here because only
// the adapter knows which flags the caller actually typed:
//
//	explicit flag  >  context in --config  >  the flag's built-in default
//
// A context sets defaults, never overrides. The alternative -- a file
// silently changing what a command line plainly says -- is the behaviour
// that makes people stop trusting config files.
//
// Values are applied through the same flagSpecs table that registers the
// flags, so the file cannot disagree with the flag about what a value means
// or about which names exist.

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/withObsrvr/stellar-ingest/internal/config"
	"github.com/withObsrvr/stellar-ingest/internal/exit"
)

// Networks maps the short names §9 uses onto passphrases.
//
// --network mainnet is what the standard's examples pass, and typing a
// passphrase by hand is how a testnet run ends up labelled mainnet: the two
// strings differ by one word in the middle.
var Networks = map[string]string{
	"mainnet":   "Public Global Stellar Network ; September 2015",
	"pubnet":    "Public Global Stellar Network ; September 2015",
	"testnet":   "Test SDF Network ; September 2015",
	"futurenet": "Test SDF Future Network ; October 2022",
}

// NetworkNames lists the accepted short names, sorted, for error messages.
func NetworkNames() string {
	names := make([]string, 0, len(Networks))
	for name := range Networks {
		names = append(names, name)
	}
	sortStrings(names)
	return strings.Join(names, ", ")
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// specByName indexes the flag table for lookup by name.
func specByName() map[string]flagSpec {
	index := make(map[string]flagSpec, len(flagSpecs))
	for _, spec := range flagSpecs {
		index[spec.Name] = spec
	}
	return index
}

// ApplyConfig loads --config, selects the context, and fills in every flag
// the caller did not type.
//
// isSet reports whether a flag was given explicitly; the two surfaces answer
// it differently -- pflag has Changed, the standard library has Visit -- so
// it is a parameter rather than something this function works out.
//
// Returns notices for the caller to print, in keeping with the rest of this
// package: the caller owns the stream.
func ApplyConfig(v *Values, isSet func(string) bool) ([]string, error) {
	if v.ConfigPath == "" {
		if v.Context != "" {
			return nil, exit.Usagef("--context %s needs --config to name a file defining it", v.Context)
		}
		return nil, applyNetwork(v, isSet)
	}

	file, err := config.Load(v.ConfigPath)
	if err != nil {
		return nil, exit.Wrap(exit.Usage, err)
	}
	name, values, err := file.Select(v.Context)
	if err != nil {
		return nil, exit.Wrap(exit.Usage, err)
	}

	index := specByName()
	var notices []string
	applied := 0
	for key, raw := range values {
		spec, known := index[key]
		if !known {
			// Loudly: a typo in a config file otherwise means a setting
			// that silently does nothing, which is how a bucket stays
			// pointed at the wrong network.
			return nil, exit.Usagef("config context %q sets unknown option %q", name, key)
		}
		if isSet(key) {
			// Explicit flag wins. Said out loud so nobody debugs a config
			// file that is being correctly ignored.
			notices = append(notices, fmt.Sprintf(
				"--%s was given on the command line, so context %q's value for it is ignored", key, name))
			continue
		}
		if err := setFromString(v, spec, raw); err != nil {
			return nil, exit.Usagef("config context %q, option %q: %v", name, key, err)
		}
		applied++
	}
	notices = append(notices, fmt.Sprintf("using context %q from %s (%d options applied)",
		name, v.ConfigPath, applied))

	// After the context, because a context may set network.
	if err := applyNetwork(v, isSet); err != nil {
		return nil, err
	}
	return notices, nil
}

// applyNetwork turns --network into a passphrase.
//
// An explicit --network-passphrase wins, so the long form stays usable for a
// network this table does not know.
func applyNetwork(v *Values, isSet func(string) bool) error {
	if v.Network == "" {
		return nil
	}
	passphrase, known := Networks[v.Network]
	if !known {
		return exit.Usagef("--network %q is not a known network; want one of %s, or pass "+
			"--network-passphrase directly", v.Network, NetworkNames())
	}
	if isSet("network-passphrase") {
		return nil
	}
	v.NetworkPassphrase = passphrase
	return nil
}

// setFromString assigns a string value through the flag table, so the
// config file and the command line parse values identically.
func setFromString(v *Values, spec flagSpec, raw string) error {
	switch target := spec.Target(v).(type) {
	case *string:
		*target = raw
	case *bool:
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("%q is not a boolean", raw)
		}
		*target = parsed
	case *uint:
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not a non-negative integer", raw)
		}
		*target = uint(parsed)
	case *int:
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("%q is not an integer", raw)
		}
		*target = parsed
	case *int64:
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not an integer", raw)
		}
		*target = parsed
	case *time.Duration:
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("%q is not a duration", raw)
		}
		*target = parsed
	default:
		return fmt.Errorf("unhandled target type %T", target)
	}
	return nil
}
