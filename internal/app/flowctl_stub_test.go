package app

// A stub flowctl control plane, so the integration is exercised rather than
// inspected.
//
// Before this, the only flowctl test was a pure-function check of failure
// classification. Everything else -- that the component registers at all,
// that it registers as a SOURCE, that chunk phases are reported in order,
// that a failure carries a classification a scheduler can act on -- was
// true by reading the code and untested. The one behaviour anybody would
// notice, that enabling flowctl makes a run refuse to start when the
// control plane is down, was discovered by running the binary rather than
// asserted anywhere.
//
// The stub implements only the three RPCs the reporter uses. Embedding
// UnimplementedControlPlaneServer means a fourth would fail loudly here if
// the reporter ever started calling one, rather than being silently absent.

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	flowctlpb "github.com/withobsrvr/flowctl/proto"
)

// stubControlPlane records what a component told it.
type stubControlPlane struct {
	flowctlpb.UnimplementedControlPlaneServer

	mu         sync.Mutex
	registered []*flowctlpb.ServiceInfo
	heartbeats int
	chunks     []*flowctlpb.ChunkRun

	// registerErr, when set, makes Register fail -- the control plane is up
	// but rejecting us, which is a different failure from it being down.
	registerErr error
}

func (s *stubControlPlane) Register(_ context.Context, info *flowctlpb.ServiceInfo) (*flowctlpb.RegistrationAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.registerErr != nil {
		return nil, s.registerErr
	}
	s.registered = append(s.registered, info)
	return &flowctlpb.RegistrationAck{ServiceId: info.ServiceId}, nil
}

func (s *stubControlPlane) Heartbeat(_ context.Context, _ *flowctlpb.ServiceHeartbeat) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats++
	return &emptypb.Empty{}, nil
}

func (s *stubControlPlane) UpsertChunkRun(_ context.Context, req *flowctlpb.UpsertChunkRunRequest) (*flowctlpb.ChunkRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append(s.chunks, req.Chunk)
	return req.Chunk, nil
}

// snapshot copies the recorded state, so assertions never read it while the
// heartbeat goroutine is still writing.
func (s *stubControlPlane) snapshot() ([]*flowctlpb.ServiceInfo, []*flowctlpb.ChunkRun) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*flowctlpb.ServiceInfo(nil), s.registered...),
		append([]*flowctlpb.ChunkRun(nil), s.chunks...)
}

// phases returns the phase of every chunk report, in the order received.
func (s *stubControlPlane) phases() []string {
	_, chunks := s.snapshot()
	out := make([]string, 0, len(chunks))
	for _, c := range chunks {
		out = append(out, c.Phase)
	}
	return out
}

// startStubControlPlane listens on a loopback port and serves until the test
// ends. A real port rather than bufconn because the component dials whatever
// FLOWCTL_ENDPOINT names -- the environment variable is part of what is
// under test.
func startStubControlPlane(t *testing.T, stub *stubControlPlane) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	flowctlpb.RegisterControlPlaneServer(server, stub)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		<-done
	})
	return listener.Addr().String()
}

// enableFlowctl points the component at a stub for the duration of a test.
func enableFlowctl(t *testing.T, endpoint string) {
	t.Helper()
	t.Setenv("ENABLE_FLOWCTL", "true")
	t.Setenv("FLOWCTL_ENDPOINT", endpoint)
	t.Setenv("FLOWCTL_COMPONENT_ID", "stellar-ingest-test")
	// Short enough that a heartbeat lands inside a five-ledger run.
	t.Setenv("FLOWCTL_HEARTBEAT_INTERVAL_MS", "50")
}
