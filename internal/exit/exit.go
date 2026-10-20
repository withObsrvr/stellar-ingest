// Package exit maps failures onto the process exit codes §3 of the CLI
// Engineering Standard requires: "0 success; documented nonzero classes for
// invalid input, missing resource, permission, timeout, cancellation,
// internal failure."
//
// Before this package every failure was log.Fatal, so every failure was exit
// 1. A caller could tell that a run failed and nothing else -- an unreadable
// credential file, a ledger range that does not exist, and a bug in the
// pusher were indistinguishable. For a backfill that is dispatched by a
// scheduler and retried automatically that distinction is the difference
// between "retry this" and "stop and page someone": retrying invalid input
// forever is how a broken flag becomes a quota bill.
//
// Class 1 deliberately keeps its historical meaning of "something went
// wrong". Everything that checks for nonzero -- Nomad, the chunk runner,
// every shell script -- is unaffected by this package; only callers that want
// to distinguish gain anything.
package exit

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"syscall"
)

// Class is a documented exit code.
type Class int

const (
	// OK is a successful run.
	OK Class = 0
	// Internal is an unexpected failure: a bug, or a dependency behaving in
	// a way the code did not anticipate. Retrying may help; it may not.
	Internal Class = 1
	// Usage is invalid input: a missing required flag, a value out of range,
	// a combination the tool rejects. Retrying the same invocation cannot
	// help, which is the point of separating it from Internal.
	Usage Class = 2
	// NotFound is a resource the run needs and cannot see: an output
	// directory, a manifest, a table.
	NotFound Class = 3
	// Permission is a credential or authorisation failure.
	Permission Class = 4
	// Conflict is a failed precondition: a range another run already holds
	// or has frozen, an attempt that already exists, a catalog whose version
	// does not match. Retry after resolving it, not immediately.
	Conflict Class = 5
	// Timeout is --timeout elapsing, or a deadline passed in by a caller.
	Timeout Class = 6
	// Unavailable is a dependency the run cannot reach: the archive, the
	// catalog, RPC, object storage. Retry with backoff -- this is the one
	// class where retrying the identical invocation is the right move.
	Unavailable Class = 7
	// DataValidation is a quality failure in data the run produced or read:
	// a ledger gap, a duplicate, a checksum or coverage mismatch, a schema
	// that does not match. It is never a retry. Something is wrong with the
	// data and a human has to look.
	DataValidation Class = 8
	// Cancelled is a run a signal or a caller stopped after a clean
	// shutdown. CLI-15 puts an interrupted run at 128+n rather than in the
	// 0-8 range, and 130 is SIGINT's. A SIGTERM reports 143; Code reads the
	// signal off the error, because by the time an error reaches here the
	// only record of which signal arrived is the one main.go attached.
	Cancelled Class = 130
)

// String names the class for diagnostics and for the documented table.
func (c Class) String() string {
	switch c {
	case OK:
		return "ok"
	case Internal:
		return "internal"
	case Usage:
		return "usage"
	case NotFound:
		return "not-found"
	case Permission:
		return "permission"
	case Conflict:
		return "conflict"
	case Timeout:
		return "timeout"
	case Unavailable:
		return "unavailable"
	case DataValidation:
		return "data-validation"
	case Cancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("unknown(%d)", int(c))
	}
}

// Error is an error that names its own exit class.
//
// Operations in internal/app return these so the adapter can choose an exit
// code without re-deriving intent from message text. Matching on strings is
// how a renamed message silently became exit 1.
type Error struct {
	Class Class
	Err   error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return e.Class.String()
	}
	return e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// newf builds a classified error. Unexported so each class has a named
// constructor and a call site reads as the intent rather than as a number.
func newf(class Class, format string, args ...any) error {
	return &Error{Class: class, Err: fmt.Errorf(format, args...)}
}

// Usagef reports invalid input: the caller passed something the tool cannot
// accept, and passing it again will fail the same way.
func Usagef(format string, args ...any) error { return newf(Usage, format, args...) }

// NotFoundf reports a resource the run needs and cannot find.
func NotFoundf(format string, args ...any) error { return newf(NotFound, format, args...) }

// Permissionf reports a credential or authorisation failure.
func Permissionf(format string, args ...any) error { return newf(Permission, format, args...) }

// Internalf reports an unexpected failure.
func Internalf(format string, args ...any) error { return newf(Internal, format, args...) }

// Conflictf reports a failed precondition: a range already claimed or
// frozen, an attempt that already exists, a version that does not match.
func Conflictf(format string, args ...any) error { return newf(Conflict, format, args...) }

// Unavailablef reports a dependency the run could not reach.
func Unavailablef(format string, args ...any) error { return newf(Unavailable, format, args...) }

// DataValidationf reports a data quality failure: a gap, a duplicate, a
// checksum or coverage mismatch, a schema that does not match.
func DataValidationf(format string, args ...any) error {
	return newf(DataValidation, format, args...)
}

// Wrap classifies an existing error, keeping it in the chain so errors.Is and
// errors.As still reach it.
func Wrap(class Class, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Class: class, Err: err}
}

// Signalled records which signal stopped the run.
//
// signal.NotifyContext cancels a context but throws away which signal did
// it, and CLI-15 distinguishes 130 from 143. The handler in main.go wraps
// the operation's error in one of these so Code can report 128+n without
// the exit package having to know about signal handling.
type Signalled struct {
	Signal syscall.Signal
	Err    error
}

func (e *Signalled) Error() string {
	if e.Err == nil {
		return e.Signal.String()
	}
	return e.Err.Error()
}

func (e *Signalled) Unwrap() error { return e.Err }

// Interrupt marks an error as having been caused by a signal.
func Interrupt(sig syscall.Signal, err error) error {
	if err == nil {
		return nil
	}
	return &Signalled{Signal: sig, Err: err}
}

// ClassOf reports the exit class for an error.
//
// An explicit class anywhere in the chain wins. Otherwise the standard
// library's own sentinels are honoured, because a context deadline and a
// permission error already carry their meaning and restating it at every call
// site is how one gets restated wrongly. Anything unrecognised is Internal:
// an unclassified failure is a failure the code did not anticipate, which is
// exactly what Internal means.
func ClassOf(err error) Class {
	if err == nil {
		return OK
	}
	var classified *Error
	if errors.As(err, &classified) {
		return classified.Class
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return Timeout
	case errors.Is(err, context.Canceled):
		return Cancelled
	case errors.Is(err, fs.ErrPermission):
		return Permission
	case errors.Is(err, fs.ErrNotExist):
		return NotFound
	case isUnreachable(err):
		return Unavailable
	}
	return Internal
}

// isUnreachable reports whether an error is a dependency the run could not
// reach, rather than one that answered and said no.
//
// Deliberately narrow: a refused connection, an unresolvable host and a
// network timeout are unambiguous, and CLI-15 makes 7 the one class where a
// scheduler retries the identical invocation. Anything broader -- treating
// every error from a storage client as Unavailable, say -- would turn a
// permanent misconfiguration into an infinite retry loop, which is the
// failure this taxonomy exists to prevent.
func isUnreachable(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// Code is the integer to hand os.Exit.
//
// A signal beats a class: a run stopped by SIGTERM exits 143 whatever the
// error underneath says, because what a scheduler needs to know is that the
// stop was ordered rather than encountered.
func Code(err error) int {
	if err == nil {
		return int(OK)
	}
	var signalled *Signalled
	if errors.As(err, &signalled) {
		return 128 + int(signalled.Signal)
	}
	return int(ClassOf(err))
}

// Classes lists every class, so documentation and tests enumerate one list
// rather than two that drift.
func Classes() []Class {
	return []Class{OK, Internal, Usage, NotFound, Permission, Conflict, Timeout, Unavailable, DataValidation, Cancelled}
}

// Exit ends the process with the class the error names. Kept here so the
// mapping lives in one place and main stays a composition root.
func Exit(err error) {
	os.Exit(Code(err))
}
