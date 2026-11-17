// Package session — handlers.go
//
// A record of client connections reported by the statements plug-in, which
// may connect without touching a VDB table and so never open a Session
// (doc 04 §4). Attached at vdb.connection.opened.track and
// vdb.connection.closed.release, priority 10.
package session

import (
	"fmt"
	"sync"
	"time"

	"github.com/virtual-db/vdb-core/internal/framework"
	"github.com/virtual-db/vdb-core/internal/payloads"
	"github.com/virtual-db/vdb-core/internal/points"
)

// Client is one client connection.
type Client struct {
	ConnID  uint64
	User    string
	Address string
	Since   time.Time
}

// Clients records client connections. Safe for concurrent use.
type Clients struct {
	mu sync.Mutex
	m  map[uint64]Client
}

// NewClients returns an empty record.
func NewClients() *Clients { return &Clients{m: map[uint64]Client{}} }

// All returns every client connection.
func (c *Clients) All() []Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Client, 0, len(c.m))
	for _, x := range c.m {
		out = append(out, x)
	}
	return out
}

// Register attaches the handlers.
func (c *Clients) Register(r framework.Registrar) error {
	for _, reg := range []struct {
		point string
		fn    framework.PointFunc
	}{
		{points.PointConnectionOpenedBuildContext, framework.BuildContext},
		{points.PointConnectionClosedBuildContext, framework.BuildContext},
		{points.PointQueryReceivedBuildContext, framework.BuildContext},
		{points.PointConnectionOpenedTrack, c.Track},
		{points.PointConnectionClosedRelease, c.Release},
	} {
		if err := r.Attach(reg.point, 10, reg.fn); err != nil {
			return fmt.Errorf("session: attach %q: %w", reg.point, err)
		}
	}
	return nil
}

// Track records an opened connection.
func (c *Clients) Track(ctx any, p any) (any, any, error) {
	pl, err := payloads.Decode[payloads.ConnectionOpenedPayload](p)
	if err != nil {
		return ctx, nil, fmt.Errorf("connection.opened.track: %w", err)
	}
	c.mu.Lock()
	c.m[pl.ConnectionID] = Client{ConnID: pl.ConnectionID, User: pl.User, Address: pl.Address, Since: time.Now()}
	c.mu.Unlock()
	return ctx, pl, nil
}

// Release forgets a closed connection.
func (c *Clients) Release(ctx any, p any) (any, any, error) {
	pl, err := payloads.Decode[payloads.ConnectionClosedPayload](p)
	if err != nil {
		return ctx, nil, fmt.Errorf("connection.closed.release: %w", err)
	}
	c.mu.Lock()
	delete(c.m, pl.ConnectionID)
	c.mu.Unlock()
	return ctx, pl, nil
}
