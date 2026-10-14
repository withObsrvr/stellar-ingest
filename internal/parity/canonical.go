// Package parity canonicalizes Bronze adapter rows so that outputs produced by
// two independent codebases — the historical extractor in stellar-ingest and
// the live extractor in stellar-postgres-ingester — can be compared field by
// field from identical authoritative LedgerCloseMeta XDR.
//
// # Why a hand-written encoder
//
// encoding/json is not usable as the canonical form here. It renders every
// number as a float64-compatible decimal, which silently loses precision on
// the int64 TOIDs and stroops that Bronze rows are mostly made of, and it
// cannot distinguish a NULL column from an empty string without per-type
// tagging. Ticket 04 requires both distinctions to survive, so every value is
// encoded with an explicit one-letter type tag and an exact textual payload.
//
// # Why this file is duplicated
//
// The two codebases pin different versions of github.com/withObsrvr/stellar-extract
// (v0.1.2 historical, v0.1.5 live) and therefore cannot be linked into one
// binary. Each side must canonicalize in its own process, which means this
// file exists byte-for-byte in both repositories. It depends on the standard
// library alone so the copy is always possible, and TestCanonicalCopyInSync
// fails when the two copies drift.
//
// Do not add a non-stdlib import to this file.
package parity

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CanonicalFormVersion identifies the encoding rules below. A comparison
// report is only meaningful when both sides carry the same version: changing
// how a value is rendered changes every digest, and an unversioned digest
// mismatch would read as a data discrepancy rather than a harness change.
const CanonicalFormVersion = "1"

// Type tags. Every encoded value starts with one, which is what keeps a NULL
// (`n`) distinct from an empty string (`s:`) and an empty list (`l:[]`).
const (
	tagNull   = "n"
	tagString = "s"
	tagInt    = "i"
	tagUint   = "u"
	tagBool   = "b"
	tagFloat  = "f"
	tagTime   = "t"
	tagBytes  = "y"
	tagList   = "l"
	tagMap    = "m"
)

// Encode renders an arbitrary Go value as its canonical string form.
//
// Pointers are followed; a nil pointer, nil interface, nil slice or nil map
// encodes as the null tag, which is deliberately distinct from the empty
// slice/map/string forms. Time is normalized to UTC and rendered with
// nanosecond precision so that a monotonic-clock reading or a non-UTC location
// on one side cannot register as a value difference.
func Encode(v any) string {
	if v == nil {
		return tagNull
	}
	return encodeValue(reflect.ValueOf(v))
}

func encodeValue(rv reflect.Value) string {
	if !rv.IsValid() {
		return tagNull
	}

	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return tagNull
		}
		return encodeValue(rv.Elem())
	}

	// time.Time is a struct, so it has to be matched before the struct case.
	if rv.Type() == reflect.TypeOf(time.Time{}) {
		t := rv.Interface().(time.Time)
		if t.IsZero() {
			// A zero time is a distinct state from "some instant in 0001".
			// Collapsing it to a formatted string would make an unset
			// timestamp compare equal to a deliberately ancient one.
			return tagTime + ":zero"
		}
		return tagTime + ":" + t.UTC().Format(time.RFC3339Nano)
	}

	switch rv.Kind() {
	case reflect.String:
		return tagString + ":" + escape(rv.String())
	case reflect.Bool:
		return tagBool + ":" + strconv.FormatBool(rv.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return tagInt + ":" + strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return tagUint + ":" + strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		// 'g' with -1 precision round-trips exactly and renders the same on
		// both sides for the same bits.
		return tagFloat + ":" + strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Slice:
		if rv.IsNil() {
			return tagNull
		}
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return tagBytes + ":" + base64.StdEncoding.EncodeToString(rv.Bytes())
		}
		return encodeList(rv)
	case reflect.Array:
		return encodeList(rv)
	case reflect.Map:
		if rv.IsNil() {
			return tagNull
		}
		return encodeMap(rv)
	case reflect.Struct:
		return encodeStruct(rv)
	default:
		// Channels, funcs and the like have no place in a Bronze row; failing
		// loudly beats emitting a value that compares equal by accident.
		return "?:" + rv.Kind().String()
	}
}

func encodeList(rv reflect.Value) string {
	// Element order is preserved. Bronze carries ordered arrays — call-graph
	// hops, auth credentials, claimants — where reordering is itself a
	// discrepancy, so this must not sort.
	parts := make([]string, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		parts[i] = encodeValue(rv.Index(i))
	}
	return tagList + ":[" + strings.Join(parts, ",") + "]"
}

func encodeMap(rv reflect.Value) string {
	keys := rv.MapKeys()
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, encodeValue(k)+"="+encodeValue(rv.MapIndex(k)))
	}
	// Go map iteration order is randomized, so the canonical form has to sort.
	sort.Strings(parts)
	return tagMap + ":{" + strings.Join(parts, ",") + "}"
}

func encodeStruct(rv reflect.Value) string {
	rt := rv.Type()
	parts := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		parts = append(parts, f.Name+"="+encodeValue(rv.Field(i)))
	}
	sort.Strings(parts)
	return tagMap + ":{" + strings.Join(parts, ",") + "}"
}

// escape makes the delimiter characters unambiguous inside a composite value.
// Without it a string containing "," or "}" could forge a list or struct
// boundary and make two different rows encode identically.
func escape(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '\\', ',', '[', ']', '{', '}', '=':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Row is one Bronze row flattened to canonical column encodings. Keys are the
// adapter's own field names: where the two sides name the same concept
// differently, that is a finding to record, not something the harness should
// paper over.
type Row map[string]string

// RowOf flattens a struct value into a Row, one entry per exported field.
//
// Only the top level is flattened. A nested struct field stays a single
// composite value, which keeps the column list aligned with what the adapter
// actually presents as a field.
func RowOf(v any) Row {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return Row{}
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return Row{"value": encodeValue(rv)}
	}

	rt := rv.Type()
	row := make(Row, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		row[f.Name] = encodeValue(rv.Field(i))
	}
	return row
}

// Rows flattens a slice of adapter rows. The argument is any so that each
// side can pass its own concrete row slice without a shared type.
func Rows(v any) []Row {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		panic(fmt.Sprintf("parity.Rows: expected slice, got %s", rv.Kind()))
	}
	out := make([]Row, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = RowOf(rv.Index(i).Interface())
	}
	return out
}

// Columns returns the union of column names across rows, sorted.
//
// It is a union rather than the first row's keys because a side could in
// principle emit heterogeneous rows; if it does, that is worth seeing rather
// than silently taking the first row as the schema.
func Columns(rows []Row) []string {
	seen := map[string]bool{}
	for _, r := range rows {
		for k := range r {
			seen[k] = true
		}
	}
	cols := make([]string, 0, len(seen))
	for k := range seen {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

// EncodeRow renders a whole row as one canonical string over the given
// columns, in the given order. A column absent from the row encodes as the
// null tag so that "column missing" and "column NULL" stay distinguishable
// only by the column list, never by a silent shift of field positions.
func EncodeRow(r Row, columns []string) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		v, ok := r[c]
		if !ok {
			v = tagNull
		}
		parts[i] = c + "=" + v
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// Digest is the stable hash of a canonical string.
func Digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// DigestMultiset hashes an unordered collection of canonical strings.
//
// Sorting first is what makes the digest a multiset digest: the two sides
// extract in different orders — historical fans out to 21 concurrent
// extractors, live runs them sequentially — so row order is not part of the
// contract being compared. Duplicates are retained, because a row emitted
// twice is a real difference from a row emitted once.
func DigestMultiset(values []string) string {
	sorted := make([]string, len(values))
	copy(sorted, values)
	sort.Strings(sorted)
	h := sha256.New()
	for _, v := range sorted {
		// Length-prefix so that concatenation cannot be ambiguous between
		// different splits of the same byte stream.
		fmt.Fprintf(h, "%d:%s", len(v), v)
	}
	return hex.EncodeToString(h.Sum(nil))
}
