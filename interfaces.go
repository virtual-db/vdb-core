// Package core is the public API of the vdb-core module: the database-agnostic
// core of VDB. It holds the Delta, the merge, transactions, locks, the catalog
// of record, the change log, foreign keys and the memory limit, plus the
// pipelines, event bus and plugin host (implementation plan doc 03).
//
// Consumers import only this package:
//
//	github.com/virtual-db/vdb-core
//
// All sub-packages under internal/ are implementation details.
package core

import "time"

// PointFunc is the handler type for all pipeline point handlers.
//
//	func(ctx any, payload any) (any, any, error)
type PointFunc func(ctx any, payload any) (any, any, error)

// EventFunc is the signature for all event subscribers.
// Returning a non-nil error is logged but does not affect other subscribers.
type EventFunc func(ctx any, payload any) error

// Config holds the settings the core owns. The engine fills it from its own
// configuration; the core reads no environment variables itself.
type Config struct {
	// PluginDir is scanned one level deep for plugin manifests at start. Empty
	// means no plugins.
	PluginDir string

	// Memory (product doc 12 §4).
	MemoryReserve float64 // fraction kept free; 0.10 by default
	DeltaMaxBytes uint64  // 0 means no fixed cap

	// Writes (product doc 10 §5).
	AutoIncrementGap uint64 // 1,000,000 by default (D-15)

	// Locks (product doc 07 §5).
	DefaultLockWait time.Duration // used when a statement sends none

	// Reads.
	CountDeclineKeys int // vdb_count_max_shadowed, 100,000 by default
	LookupBatch      int // vdb_lookup_batch, 1,000 by default
}

// Server is the interface the framework calls to manage the engine.
// A value satisfying Server must be passed to App.UseDriver before App.Run.
type Server interface {
	// Run starts the engine and blocks until Stop is called or a fatal error occurs.
	Run() error
	// Stop signals Run to return.
	Stop() error
}

// SourceProvider is implemented by a Server that reads a Source. UseDriver
// registers its Source with the transaction manager; without one, every table
// is local (implementation plan doc 03 §2).
type SourceProvider interface {
	Source() Source
}

// MemoryProber is implemented by a Server that runs processes besides this
// one. UseDriver hands its probe to the memory guard; without one, the guard
// measures this process only.
type MemoryProber interface {
	MemoryProbe() (rss uint64, err error)
}
