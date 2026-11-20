// Package lifecycle provides pipeline handlers for the framework's lifecycle
// pipelines: vdb.context.create, vdb.server.start, and vdb.server.stop.
package lifecycle

import (
	"github.com/virtual-db/vdb-core/internal/framework"
	"github.com/virtual-db/vdb-core/internal/plugin"
	"github.com/virtual-db/vdb-core/internal/types"
)

// Server is the minimal shape lifecycle needs from the DB server.
// Defined here so lifecycle does not import the root vdb-core package.
// core.Server satisfies this interface structurally.
type Server interface {
	Run() error
	Stop() error
}

// App is the subset of core.App that lifecycle handlers mutate or observe.
// core.App satisfies this interface. Tests supply a stub.
type App interface {
	Bus() *framework.Bus
	Pipe() *framework.Pipeline
	SetGlobal(framework.GlobalContext)
	GetServer() Server
	ServerErrCh() chan<- error
	Plugins() *plugin.Manager
	// Restore replays a restored change set into the Delta and the catalog.
	Restore(cs *types.ChangeSet) error
}
