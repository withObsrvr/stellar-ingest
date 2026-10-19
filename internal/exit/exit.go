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
	"os"
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
	// Timeout is --timeout elapsing, or a deadline passed in by a caller.
	Timeout Class = 5
	// Cancelled is SIGINT, SIGTERM, or a caller cancelling the context. It
	// is distinct from Timeout because it means somebody asked for the stop.
	Cancelled Class = 6
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
	case Timeout:
		return "timeout"
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

// Wrap classifies an existing error, keeping it in the chain so errors.Is and
// errors.As still reach it.
func Wrap(class Class, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Class: class, Err: err}
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
	}
	return Internal
}

// Code is the integer to hand os.Exit.
func Code(err error) int { return int(ClassOf(err)) }

// Classes lists every class, so documentation and tests enumerate one list
// rather than two that drift.
func Classes() []Class {
	return []Class{OK, Internal, Usage, NotFound, Permission, Timeout, Cancelled}
}

// Exit ends the process with the class the error names. Kept here so the
// mapping lives in one place and main stays a composition root.
func Exit(err error) {
	os.Exit(Code(err))
}
