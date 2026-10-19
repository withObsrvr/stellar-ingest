// Package logging is the tool's narration: progress, phase headers, warnings
// and diagnostics, all on stderr.
//
// §3 of the CLI Engineering Standard divides the two streams: "stdout
// contains results; stderr contains logs, progress, warnings, diagnostics.
// Preserve composable pipes." This tool had them mixed -- the banner, the
// per-phase headers and every summary went to stdout alongside what a caller
// would actually want to parse -- so `stellar-ingest ... | jq` could never
// work, and neither could anything else downstream.
//
// Built on log/slog as §6 asks. The level is what slog is doing for us:
// --quiet and --verbose are a level, not a set of conditionals scattered
// through the call sites. The handler renders the message plainly rather than
// as key=value, because an aligned banner is the thing a person reads and
// slog's default format would destroy it; attributes are appended only at
// verbose, where structure is what was asked for.
//
// It knows nothing about terminals. Whether colour is appropriate depends on
// whether stderr is a TTY, on NO_COLOR, and on --no-color, and deciding that
// is the adapter's job -- see cli.ResolveLogging. A package the application
// layer depends on must not reach for os.Stderr to answer a presentation
// question.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// Level is how much narration to emit.
type Level int

const (
	// LevelQuiet emits warnings and errors only. Progress is suppressed;
	// failures are never suppressed, because a silent failure is worse than
	// a noisy one.
	LevelQuiet Level = iota
	// LevelNormal emits progress, phase headers and summaries.
	LevelNormal
	// LevelVerbose adds per-step detail and renders structured attributes.
	LevelVerbose
)

// String names the level for diagnostics.
func (l Level) String() string {
	switch l {
	case LevelQuiet:
		return "quiet"
	case LevelNormal:
		return "normal"
	case LevelVerbose:
		return "verbose"
	default:
		return fmt.Sprintf("unknown(%d)", int(l))
	}
}

// slogLevel maps onto slog's levels so the handler does the filtering.
func (l Level) slogLevel() slog.Level {
	switch l {
	case LevelQuiet:
		return slog.LevelWarn
	case LevelVerbose:
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

// Options configures a Logger. Err defaults to io.Discard rather than
// os.Stderr: a zero Options must not write anywhere a test did not ask for.
type Options struct {
	Level Level
	// Color enables ANSI styling. The caller decides; see the package
	// comment.
	Color bool
	Err   io.Writer
}

// Logger narrates to stderr at a level.
//
// Every method is nil-safe. Operations take a Logger from their caller and a
// test that does not care about output passes nothing; a nil check here is
// cheaper than a guard at each of sixty call sites.
type Logger struct {
	log   *slog.Logger
	level Level
	color bool
}

// New builds a Logger.
func New(opts Options) *Logger {
	out := opts.Err
	if out == nil {
		out = io.Discard
	}
	handler := &humanHandler{
		out:     out,
		min:     opts.Level.slogLevel(),
		color:   opts.Color,
		verbose: opts.Level == LevelVerbose,
	}
	return &Logger{log: slog.New(handler), level: opts.Level, color: opts.Color}
}

// Discard is a Logger that writes nothing, for tests and for callers that
// have no stream to write to.
func Discard() *Logger { return New(Options{}) }

// Level reports the configured level.
func (l *Logger) Level() Level {
	if l == nil {
		return LevelQuiet
	}
	return l.level
}

// Slog exposes the underlying structured logger, for diagnostics that carry
// attributes rather than prose.
func (l *Logger) Slog() *slog.Logger {
	if l == nil {
		return slog.New(&humanHandler{out: io.Discard, min: slog.LevelError})
	}
	return l.log
}

// Info reports progress. Suppressed by --quiet.
func (l *Logger) Info(format string, args ...any) {
	l.emit(slog.LevelInfo, format, args...)
}

// Detail reports per-step detail. Emitted only under --verbose.
func (l *Logger) Detail(format string, args ...any) {
	l.emit(slog.LevelDebug, format, args...)
}

// Warn reports something that did not stop the run. Survives --quiet,
// because a warning the operator cannot see is a warning that does not
// exist.
func (l *Logger) Warn(format string, args ...any) {
	l.emit(slog.LevelWarn, format, args...)
}

// Error reports a failure. Survives --quiet.
func (l *Logger) Error(format string, args ...any) {
	l.emit(slog.LevelError, format, args...)
}

// Line emits a blank separator. Suppressed by --quiet, where vertical
// whitespace between suppressed sections is just blank output.
func (l *Logger) Line() {
	l.emit(slog.LevelInfo, "")
}

// Section emits a phase header: the "=== DuckLake Push ===" shape this tool
// has always used, bolded when colour is on.
func (l *Logger) Section(title string) {
	if l == nil {
		return
	}
	l.emit(slog.LevelInfo, "%s", l.paint(bold, "=== "+title+" ==="))
}

// Field emits one aligned "Label:    value" line of a banner. The width is
// fixed so successive calls line up without each call site padding its own
// label, which is how two of them came to disagree by a space.
func (l *Logger) Field(label, format string, args ...any) {
	l.emit(slog.LevelInfo, "%-18s %s", label+":", fmt.Sprintf(format, args...))
}

func (l *Logger) emit(level slog.Level, format string, args ...any) {
	if l == nil {
		return
	}
	message := format
	if len(args) > 0 {
		message = fmt.Sprintf(format, args...)
	}
	l.log.Log(context.Background(), level, message)
}

// ANSI codes, kept here so nothing else in the tree hardcodes an escape.
const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	yellow = "\033[33m"
	red    = "\033[31m"
	dim    = "\033[2m"
)

// paint styles text when colour is enabled.
func (l *Logger) paint(code, text string) string {
	if l == nil || !l.color || text == "" {
		return text
	}
	return code + text + reset
}

// humanHandler renders slog records as the lines a person reads.
//
// Not slog.TextHandler: that would turn every progress line into
// time=... level=INFO msg="..." and the banner into something unreadable.
// What the level system is for here is filtering, and that is what this
// keeps.
type humanHandler struct {
	out     io.Writer
	min     slog.Level
	color   bool
	verbose bool
	attrs   []slog.Attr
	group   string
}

func (h *humanHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.min
}

func (h *humanHandler) Handle(_ context.Context, record slog.Record) error {
	var line strings.Builder
	line.WriteString(h.prefix(record.Level))
	line.WriteString(record.Message)

	// Attributes are detail. At normal level a progress line should read as a
	// sentence; under --verbose the structure is the point.
	if h.verbose {
		for _, attr := range h.attrs {
			appendAttr(&line, h.group, attr)
		}
		record.Attrs(func(attr slog.Attr) bool {
			appendAttr(&line, h.group, attr)
			return true
		})
	}

	line.WriteByte('\n')
	_, err := io.WriteString(h.out, line.String())
	return err
}

// prefix labels the lines that need labelling. Progress is unlabelled: it is
// the normal case, and "info:" on every line of a banner is noise.
func (h *humanHandler) prefix(level slog.Level) string {
	switch {
	case level >= slog.LevelError:
		return h.paint(red, "error: ")
	case level >= slog.LevelWarn:
		return h.paint(yellow, "warning: ")
	case level <= slog.LevelDebug:
		return h.paint(dim, "debug: ")
	default:
		return ""
	}
}

func (h *humanHandler) paint(code, text string) string {
	if !h.color {
		return text
	}
	return code + text + reset
}

func appendAttr(line *strings.Builder, group string, attr slog.Attr) {
	if attr.Equal(slog.Attr{}) {
		return
	}
	key := attr.Key
	if group != "" {
		key = group + "." + key
	}
	fmt.Fprintf(line, " %s=%v", key, attr.Value)
}

func (h *humanHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &next
}

func (h *humanHandler) WithGroup(name string) slog.Handler {
	next := *h
	if name != "" {
		if next.group == "" {
			next.group = name
		} else {
			next.group = next.group + "." + name
		}
	}
	return &next
}
