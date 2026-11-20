// Package core_test — tests/interfaces_test.go
//
// Compile-time and runtime checks of the public API's shape: Server,
// DriverAPI, UseDriver (with its optional Source and probe) and HasHandlers.
package core_test

import (
	"testing"

	core "github.com/virtual-db/vdb-core"
)

// stubServer satisfies core.Server.
type stubServer struct{}

func (s *stubServer) Run() error  { return nil }
func (s *stubServer) Stop() error { return nil }

var _ core.Server = (*stubServer)(nil)

// TestUseDriverReturnsApp verifies UseDriver supports method chaining.
func TestUseDriverReturnsApp(t *testing.T) {
	app := core.New(core.Config{})
	if got := app.UseDriver(&stubServer{}); got != app {
		t.Fatal("UseDriver must return the same *App for method chaining")
	}
}

// sourceServer is a Server that supplies a Source and a memory probe.
type sourceServer struct {
	src    core.Source
	probed bool
}

func (s *sourceServer) Run() error          { return nil }
func (s *sourceServer) Stop() error         { return nil }
func (s *sourceServer) Source() core.Source { return s.src }
func (s *sourceServer) MemoryProbe() (uint64, error) {
	s.probed = true
	return 0, nil
}

// TestUseDriverTakesSourceAndProbe verifies UseDriver accepts a Server that
// also supplies a Source and a memory probe, and still chains.
func TestUseDriverTakesSourceAndProbe(t *testing.T) {
	app := core.New(core.Config{})
	srv := &sourceServer{}
	if got := app.UseDriver(srv); got != app {
		t.Fatal("UseDriver must return the same *App")
	}
}

// TestDriverAPIQueryReceivedPassesThrough confirms that QueryReceived returns
// the statement unchanged when nothing is attached to intercept.
func TestDriverAPIQueryReceivedPassesThrough(t *testing.T) {
	app := core.New(core.Config{})
	q, err := app.DriverAPI().QueryReceived(1, "SELECT 1", "testdb")
	if err != nil {
		t.Fatalf("QueryReceived: %v", err)
	}
	if q != "SELECT 1" {
		t.Fatalf("QueryReceived: got %q", q)
	}
}

// TestHasHandlersIgnoresBuiltins checks that built-in handlers do not count
// and an attached handler does.
func TestHasHandlersIgnoresBuiltins(t *testing.T) {
	app := core.New(core.Config{})
	if app.HasHandlers("vdb.write.insert.apply") {
		t.Fatal("built-in apply handler must not count")
	}
	app.Attach("vdb.query.received.intercept", 5, func(ctx, p any) (any, any, error) { return ctx, p, nil })
	if !app.HasHandlers("vdb.query.received.intercept") {
		t.Fatal("attached handler must count")
	}
}
