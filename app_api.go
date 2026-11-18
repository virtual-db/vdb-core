package core

import (
	"fmt"
	"log/slog"

	"github.com/virtual-db/vdb-core/internal/framework"
)

// UseDriver registers s as the database server. The composition root calls
// DriverAPI() explicitly and passes the result to the driver constructor before
// calling UseDriver.
//
// Panics if called after Run has started. Returns *App for method chaining.
func (a *App) UseDriver(s Server) *App {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		panic("core: UseDriver called after Run")
	}
	a.server = s
	return a
}

// UseSource registers the engine's Source adapter. Must be called before
// Run. Panics after Run. Returns *App for method chaining.
func (a *App) UseSource(s Source) *App {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		panic("core: UseSource called after Run")
	}
	a.source = s
	a.txn.SetSource(s)
	return a
}

// HasHandlers reports whether any handler other than the built-in ones is
// attached to a point. The engine uses it to make vdb.query.received
// synchronous only when something is attached to intercept.
func (a *App) HasHandlers(point string) bool {
	return a.pipe.HasHandlers(point)
}

// DriverAPI returns the framework's DriverAPI implementation. Pass this to the
// driver constructor so the driver can call back into the framework.
//
//	api    := app.DriverAPI()
//	driver := mysql.NewDriver(cfg, api)
//	app.UseDriver(driver)
func (a *App) DriverAPI() DriverAPI {
	return a.apiImpl
}

// Attach registers fn as a handler at the named pipeline point with the given
// priority. Panics if point is not a declared vdb.* point. Panics if called
// after Run has started. Returns *App for method chaining.
func (a *App) Attach(point string, priority int, fn PointFunc) *App {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		panic("core: Attach called after Run")
	}
	a.mu.Unlock()
	if err := a.pipe.Attach(point, priority, framework.PointFunc(fn)); err != nil {
		panic(fmt.Sprintf("core: Attach: unknown point %q: %v", point, err))
	}
	return a
}

// Subscribe registers fn as a handler for the named event. Unknown event names
// are logged as a warning and silently dropped. Panics if called after Run.
// Returns *App for method chaining.
func (a *App) Subscribe(event string, fn EventFunc) *App {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		panic("core: Subscribe called after Run")
	}
	a.mu.Unlock()
	if err := a.bus.Subscribe(event, framework.EventFunc(fn)); err != nil {
		slog.Warn(fmt.Sprintf("core: Subscribe: %v", err), "pkg", "core")
	}
	return a
}

// DeclareEvent declares a plugin-owned event on the app's event bus so that
// other components can subscribe to it before App.Run starts the server.
func (a *App) DeclareEvent(event string) {
	a.bus.DeclareEvent(event)
}

// DeclarePipeline declares a plugin-owned pipeline in the app's pipeline registry
// so other components can attach handlers to its points.
func (a *App) DeclarePipeline(name string, pointNames []string) {
	a.pipe.DeclarePipeline(name, pointNames)
}

// Emit relays payload to the named event on the app's event bus.
func (a *App) Emit(event string, payload any) {
	a.bus.Emit(event, payload)
}

// Process runs the named pipeline with the given payload.
func (a *App) Process(pipeline string, payload any) (any, error) {
	return a.global.Pipeline().Process(pipeline, payload)
}
