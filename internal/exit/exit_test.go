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
	"os"
	"regexp"
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
		"Permission": Permission, "Timeout": Timeout, "Cancelled": Cancelled,
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
