package exit

// What these guard, in order of how likely the mistake is:
//
//  1. Two classes sharing a code. The whole point of the package is that a
//     caller can tell failures apart; two classes with the same number is
//     the package silently not working.
//  2. A class declared and left out of Classes(). The documented table and
//     every test that enumerates classes read that list, so a forgotten
//     entry is a class nobody checks.
//  3. Classification lost through wrapping. Operations wrap errors with
//     context as they return, so a class that survives one %w and not three
//     works in a unit test and fails in production.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"regexp"
	"syscall"
	"testing"
)

func TestClassesHaveDistinctCodesAndNames(t *testing.T) {
	byCode := map[Class]bool{}
	byName := map[string]bool{}
	for _, c := range Classes() {
		if byCode[c] {
			t.Errorf("exit code %d is used by more than one class; callers cannot tell them apart", int(c))
		}
		byCode[c] = true

		name := c.String()
		if byName[name] {
			t.Errorf("class name %q is used by more than one class", name)
		}
		byName[name] = true

		if matched, _ := regexp.MatchString(`^unknown`, name); matched {
			t.Errorf("class %d has no name; it is in Classes() but not in String()", int(c))
		}
	}
}

// Declaring a class and forgetting Classes() leaves it out of the documented
// table and out of every enumerating test. Read from the source so the list
// cannot drift from the declarations.
func TestEveryDeclaredClassIsInClasses(t *testing.T) {
	source, err := os.ReadFile("exit.go")
	if err != nil {
		t.Fatalf("read own source: %v", err)
	}
	declared := regexp.MustCompile(`(?m)^\t([A-Z]\w*) Class = `).FindAllStringSubmatch(string(source), -1)
	if len(declared) == 0 {
		t.Fatal("found no class declarations; this test is broken, not the code")
	}

	listed := map[string]bool{}
	for _, c := range Classes() {
		listed[c.String()] = true
	}
	// The declaration is Go-cased (NotFound); String is kebab (not-found).
	// Compare on the code instead, which both sides agree on.
	byName := map[string]Class{
		"OK": OK, "Internal": Internal, "Usage": Usage, "NotFound": NotFound,
		"Permission": Permission, "Conflict": Conflict, "Timeout": Timeout,
		"Unavailable": Unavailable, "DataValidation": DataValidation,
		"Cancelled": Cancelled,
	}
	for _, match := range declared {
		name := match[1]
		class, known := byName[name]
		if !known {
			t.Errorf("class %s is declared in exit.go but this test does not know it; add it here "+
				"and to Classes()", name)
			continue
		}
		if !listed[class.String()] {
			t.Errorf("class %s is declared but missing from Classes(); the documented table and "+
				"every enumerating test will skip it", name)
		}
	}
	if len(declared) != len(Classes()) {
		t.Errorf("%d classes declared, %d in Classes()", len(declared), len(Classes()))
	}
}

func TestClassOf(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"nil is success", nil, OK},
		{"usage", Usagef("--start is required"), Usage},
		{"not found", NotFoundf("no manifest"), NotFound},
		{"permission", Permissionf("bad key"), Permission},
		{"internal", Internalf("boom"), Internal},
		{"deadline sentinel", context.DeadlineExceeded, Timeout},
		{"cancel sentinel", context.Canceled, Cancelled},
		{"fs permission sentinel", fs.ErrPermission, Permission},
		{"fs not-exist sentinel", fs.ErrNotExist, NotFound},
		{"unclassified is internal", errors.New("something odd"), Internal},
		{"wrapped sentinel", fmt.Errorf("push: %w", context.DeadlineExceeded), Timeout},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassOf(c.err); got != c.want {
				t.Errorf("ClassOf = %v, want %v", got, c.want)
			}
			if got := Code(c.err); got != int(c.want) {
				t.Errorf("Code = %d, want %d", got, int(c.want))
			}
		})
	}
}

// Operations add context as an error returns. A class that does not survive
// that is a class that works in tests and not in the program.
func TestClassSurvivesDeepWrapping(t *testing.T) {
	err := Usagef("--chunk-concurrency requires --chunk-size")
	for i := 0; i < 3; i++ {
		err = fmt.Errorf("layer %d: %w", i, err)
	}
	if got := ClassOf(err); got != Usage {
		t.Errorf("ClassOf after 3 wraps = %v, want %v", got, Usage)
	}
	if !errors.Is(err, err) {
		t.Error("errors.Is broken on the chain")
	}
}

// An outer layer that re-classifies must win: it has more context about what
// the failure means to the caller than the layer that produced it.
func TestOutermostClassWins(t *testing.T) {
	inner := Internalf("connection reset")
	outer := Wrap(Permission, fmt.Errorf("attach catalog: %w", inner))
	if got := ClassOf(outer); got != Permission {
		t.Errorf("ClassOf = %v, want %v (the outer classification)", got, Permission)
	}
}

// The underlying error must stay reachable, or callers lose the ability to
// match on a sentinel they care about.
func TestWrapKeepsTheChainReachable(t *testing.T) {
	sentinel := errors.New("original")
	err := Wrap(Usage, fmt.Errorf("context: %w", sentinel))
	if !errors.Is(err, sentinel) {
		t.Error("errors.Is cannot reach the wrapped error; the chain was broken")
	}
	if got := err.Error(); got == "" {
		t.Error("Error() lost the message")
	}
}

func TestWrapOfNilIsNil(t *testing.T) {
	if err := Wrap(Usage, nil); err != nil {
		t.Errorf("Wrap(class, nil) = %v, want nil; a nil error must not become a failure", err)
	}
}

// Success must be zero. If this ever drifts every successful run starts
// failing its caller.
func TestSuccessIsZero(t *testing.T) {
	if int(OK) != 0 {
		t.Errorf("OK = %d, want 0", int(OK))
	}
	if Code(nil) != 0 {
		t.Errorf("Code(nil) = %d, want 0", Code(nil))
	}
}

// Everything that checks "nonzero means failure" -- Nomad, the chunk runner,
// every shell script -- depends on this.
func TestEveryFailureClassIsNonZero(t *testing.T) {
	for _, c := range Classes() {
		if c == OK {
			continue
		}
		if int(c) == 0 {
			t.Errorf("class %s is zero, so a failure would report success", c)
		}
	}
}

// The codes are a published contract (CLI-15 of the OBSRVR Engineering
// Standard), and a scheduler's retry policy is written against them. Pinning
// the integers means a reordering of the const block cannot quietly turn
// "timed out, retry" into "conflict, resolve first".
func TestCodesMatchTheStandardTable(t *testing.T) {
	want := map[string]int{
		"ok": 0, "internal": 1, "usage": 2, "not-found": 3, "permission": 4,
		"conflict": 5, "timeout": 6, "unavailable": 7, "data-validation": 8,
		"cancelled": 130,
	}
	for _, c := range Classes() {
		if got, ok := want[c.String()]; !ok {
			t.Errorf("class %q is not in the standard's table", c.String())
		} else if int(c) != got {
			t.Errorf("%s exits %d, the standard says %d", c.String(), int(c), got)
		}
	}
	if len(want) != len(Classes()) {
		t.Errorf("the table has %d entries, Classes() has %d", len(want), len(Classes()))
	}
}

// 130 and 143 are 128+n, and which one depends on the signal. Reporting
// SIGTERM as 130 would tell a supervisor the operator pressed Ctrl-C when
// in fact the supervisor itself asked the process to stop.
func TestSignalBeatsClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"SIGINT", Interrupt(syscall.SIGINT, context.Canceled), 130},
		{"SIGTERM", Interrupt(syscall.SIGTERM, context.Canceled), 143},
		{"signal wins over a classified error", Interrupt(syscall.SIGTERM, Usagef("bad flag")), 143},
		{"wrapped signal still counts", fmt.Errorf("extract: %w",
			Interrupt(syscall.SIGINT, context.Canceled)), 130},
		{"no signal, bare cancellation", context.Canceled, 130},
		{"no signal, deadline", context.DeadlineExceeded, 6},
		{"nil", nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Code(c.err); got != c.want {
				t.Errorf("Code = %d, want %d", got, c.want)
			}
		})
	}
}

// The three classes added for CLI-15 must survive wrapping like the others,
// since every one of them is produced deep in an operation and read at the
// top of main.
func TestNewClassesSurviveWrapping(t *testing.T) {
	for _, c := range []struct {
		class Class
		err   error
	}{
		{Conflict, Conflictf("range already frozen")},
		{Unavailable, Unavailablef("archive unreachable")},
		{DataValidation, DataValidationf("ledger gap at 64000005")},
	} {
		wrapped := fmt.Errorf("run: %w", fmt.Errorf("extract: %w", c.err))
		if got := ClassOf(wrapped); got != c.class {
			t.Errorf("%s lost through two wraps: got %s", c.class, got)
		}
	}
}

// Code 7 is the one class the standard says a scheduler may retry with the
// identical invocation, so what qualifies has to stay narrow. A refused
// connection is a dependency being down; a permission error from the same
// client is not, and retrying it forever is how a misconfiguration becomes
// a bill.
func TestUnreachableIsNarrow(t *testing.T) {
	unreachable := []error{
		fmt.Errorf("dial catalog: %w", syscall.ECONNREFUSED),
		fmt.Errorf("lookup archive: %w", &net.DNSError{Err: "no such host", IsNotFound: true}),
		fmt.Errorf("read ledger: %w", syscall.EHOSTUNREACH),
	}
	for _, err := range unreachable {
		if got := ClassOf(err); got != Unavailable {
			t.Errorf("%v classified %s, want unavailable", err, got)
		}
	}

	notUnreachable := []error{
		fmt.Errorf("catalog: %w", fs.ErrPermission),
		errors.New("bucket does not exist"),
		Usagef("--bucket is required"),
	}
	for _, err := range notUnreachable {
		if got := ClassOf(err); got == Unavailable {
			t.Errorf("%v classified unavailable; retrying it cannot help", err)
		}
	}
}
