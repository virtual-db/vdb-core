// Package catalog is the catalog of record (implementation plan doc 06 §1):
// every table with its kind, definition, version, indexes and foreign keys,
// every client and Source object in commit order, and the providers of
// provided tables. The engine rebuilds its own server's objects from it at every start.
//
// Catalog is shared by every session and guarded by one RWMutex.
package catalog

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/virtual-db/vdb-core/internal/types"
)

// Hooks are called after the catalog changes, outside its lock.
type Hooks struct {
	Loaded      func(t types.TableDef)
	Invalidated func(t types.TableDef)
	// Record commits a change set of its own (captured objects, Source changes).
	Record func(cs *types.ChangeSet) error
}

// Catalog is the catalog of record. Safe for concurrent use.
type Catalog struct {
	mu        sync.RWMutex
	tables    map[types.TableID]*types.TableDef
	byName    map[string]types.TableID
	objects   []types.Object
	providers map[types.TableID]types.RowProvider
	nextID    types.TableID
	objSeq    uint64
	hooks     Hooks
}

// New returns an empty catalog.
func New() *Catalog {
	return &Catalog{
		tables:    map[types.TableID]*types.TableDef{},
		byName:    map[string]types.TableID{},
		providers: map[types.TableID]types.RowProvider{},
		nextID:    1,
	}
}

// SetHooks installs the hooks. Called once by the core's wiring.
func (c *Catalog) SetHooks(h Hooks) { c.hooks = h }

func nameKey(ns []string, name string) string {
	return strings.ToLower(strings.Join(ns, "\x00")) + "\x00\x00" + strings.ToLower(name)
}

// Tables returns every table, ordered by id.
func (c *Catalog) Tables() []types.TableDef {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]types.TableDef, 0, len(c.tables))
	for _, t := range c.tables {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Table returns a table by id.
func (c *Catalog) Table(id types.TableID) (types.TableDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.tables[id]
	if !ok {
		return types.TableDef{}, false
	}
	return *t, true
}

// TableByName returns a table by database and name, case-insensitively.
func (c *Catalog) TableByName(ns []string, name string) (types.TableDef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	id, ok := c.byName[nameKey(ns, name)]
	if !ok {
		return types.TableDef{}, false
	}
	return *c.tables[id], true
}

// Objects returns every object in commit order.
func (c *Catalog) Objects() []types.Object {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]types.Object(nil), c.objects...)
}

// NextID reserves a table id for a table being created.
func (c *Catalog) NextID() types.TableID {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	return id
}

// Load merges Source definitions in (discovery at start). A table already in
// the catalog keeps its id, kind when local or detached, and local changes.
func (c *Catalog) Load(defs []types.TableDef, objs []types.Object) error {
	var loaded []types.TableDef
	c.mu.Lock()
	for _, d := range defs {
		d := d
		if id, ok := c.byName[nameKey(d.Namespace, d.Name)]; ok {
			cur := c.tables[id]
			d.ID = id
			if cur.Kind == types.Local || cur.Kind == types.Detached {
				d.Kind = cur.Kind
			}
			d.LocalChanges = cur.LocalChanges
			if cur.Engine != nil && len(cur.LocalChanges) > 0 {
				d.Engine = cur.Engine // the definition with the local changes
			}
			d.Version = cur.Version + 1
		} else {
			if d.ID == 0 {
				d.ID = c.nextID
				c.nextID++
			} else if d.ID >= c.nextID {
				c.nextID = d.ID + 1
			}
			if d.Version == 0 {
				d.Version = 1
			}
		}
		c.put(&d)
		loaded = append(loaded, d)
	}
	for _, o := range objs {
		c.addObjectLocked(o)
	}
	c.mu.Unlock()
	if c.hooks.Loaded != nil {
		for _, d := range loaded {
			c.hooks.Loaded(d)
		}
	}
	return nil
}

func (c *Catalog) put(d *types.TableDef) {
	if old, ok := c.tables[d.ID]; ok {
		delete(c.byName, nameKey(old.Namespace, old.Name))
	}
	c.tables[d.ID] = d
	c.byName[nameKey(d.Namespace, d.Name)] = d.ID
}

func (c *Catalog) addObjectLocked(o types.Object) {
	for i, x := range c.objects {
		if x.Kind == o.Kind && strings.EqualFold(strings.Join(x.Namespace, "."), strings.Join(o.Namespace, ".")) && strings.EqualFold(x.Name, o.Name) {
			c.objects = append(c.objects[:i], c.objects[i+1:]...)
			break
		}
	}
	if o.Dropped {
		return
	}
	c.objSeq++
	o.Seq = c.objSeq
	c.objects = append(c.objects, o)
}

// ApplySource applies a Source change outside any client transaction.
func (c *Catalog) ApplySource(ch types.SourceChange) (types.TableID, error) {
	var inval *types.TableDef
	var id types.TableID
	c.mu.Lock()
	switch ch.Kind {
	case types.SourceTableAdded:
		d := ch.Table
		if existing, ok := c.byName[nameKey(d.Namespace, d.Name)]; ok {
			d.ID = existing
		} else {
			d.ID = c.nextID
			c.nextID++
		}
		d.Version++
		c.put(&d)
		id = d.ID
		inval = &d
	case types.SourceTableChanged, types.SourceStatsRefreshed:
		cur, ok := c.tables[ch.ID]
		if !ok {
			c.mu.Unlock()
			return 0, fmt.Errorf("catalog.apply_source: unknown table %d", ch.ID)
		}
		d := ch.Table
		d.ID = cur.ID
		if ch.Kind == types.SourceTableChanged {
			d.Version = cur.Version + 1
		} else {
			d.Version = cur.Version
		}
		if d.Kind == types.Mirrored && (cur.Kind == types.Local || cur.Kind == types.Detached) {
			d.Kind = cur.Kind
		}
		c.put(&d)
		id = d.ID
		if ch.Kind == types.SourceTableChanged {
			inval = &d
		}
	case types.SourceTableDropped:
		cur, ok := c.tables[ch.ID]
		if ok {
			delete(c.byName, nameKey(cur.Namespace, cur.Name))
			delete(c.tables, ch.ID)
			delete(c.providers, ch.ID)
			inval = cur
		}
		id = ch.ID
	case types.SourceTableRenamed:
		cur, ok := c.tables[ch.ID]
		if !ok {
			c.mu.Unlock()
			return 0, fmt.Errorf("catalog.apply_source: unknown table %d", ch.ID)
		}
		d := *cur
		d.Namespace, d.Name = ch.Table.Namespace, ch.Table.Name
		if ch.Table.Source != nil {
			d.Source = ch.Table.Source
		}
		if ch.Table.Engine != nil {
			d.Engine = ch.Table.Engine
		}
		d.Version++
		c.put(&d)
		id = d.ID
		inval = &d
	case types.SourceObjectChanged:
		o := ch.Object
		o.Origin = types.ObjectFromSource
		c.addObjectLocked(o)
	case types.SourceObjectDropped:
		o := ch.Object
		o.Dropped = true
		c.addObjectLocked(o)
	}
	c.mu.Unlock()
	if inval != nil && c.hooks.Invalidated != nil {
		c.hooks.Invalidated(*inval)
	}
	return id, nil
}

// Apply applies a DDL change staged in a transaction, at its commit.
func (c *Catalog) Apply(ch types.CatalogChange) error {
	var inval *types.TableDef
	c.mu.Lock()
	switch ch.Kind {
	case types.CreateTable:
		d := ch.Table
		if d.ID == 0 {
			d.ID = c.nextID
			c.nextID++
		}
		if d.Version == 0 {
			d.Version = 1
		}
		c.put(&d)
		inval = &d
	case types.DropTable:
		if cur, ok := c.tables[ch.ID]; ok {
			delete(c.byName, nameKey(cur.Namespace, cur.Name))
			delete(c.tables, ch.ID)
			inval = cur
		}
	case types.RenameTable:
		cur, ok := c.tables[ch.ID]
		if !ok {
			c.mu.Unlock()
			return fmt.Errorf("catalog.apply: rename of unknown table %d", ch.ID)
		}
		d := *cur
		d.Namespace, d.Name = ch.NewNamespace, ch.NewName
		if ch.Table.Engine != nil {
			d.Engine = ch.Table.Engine
		}
		d.Version++
		c.put(&d)
		inval = &d
	case types.DetachTable:
		if cur, ok := c.tables[ch.ID]; ok && cur.Kind == types.Mirrored {
			d := *cur
			d.Kind = types.Detached
			d.Version++
			c.put(&d)
			inval = &d
		}
	case types.AlterTable:
		cur, ok := c.tables[ch.ID]
		if !ok {
			c.mu.Unlock()
			return fmt.Errorf("catalog.apply: alter of unknown table %d", ch.ID)
		}
		d := ch.Table
		d.ID, d.Kind, d.Source = cur.ID, cur.Kind, cur.Source
		if ch.Table.Source != nil {
			d.Source = ch.Table.Source
		}
		d.LocalChanges = cur.LocalChanges
		if ch.Local != nil && !ch.Source && cur.Kind == types.Mirrored {
			d.LocalChanges = append(append([]types.LocalChange(nil), cur.LocalChanges...), *ch.Local)
		}
		d.Version = cur.Version + 1
		c.put(&d)
		inval = &d
	}
	c.mu.Unlock()
	if inval != nil && c.hooks.Invalidated != nil {
		c.hooks.Invalidated(*inval)
	}
	return nil
}

// RecordObject records a captured view, routine or trigger statement and
// commits its own change set before returning.
func (c *Catalog) RecordObject(o types.Object) error {
	o.Origin = types.ObjectClient
	if c.hooks.Record != nil {
		if err := c.hooks.Record(&types.ChangeSet{Kind: types.ChangeCaptured, Objects: []types.Object{o}}); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.addObjectLocked(o)
	c.mu.Unlock()
	return nil
}

// RestoreObject adds an object from a restored change set without recording it again.
func (c *Catalog) RestoreObject(o types.Object) {
	c.mu.Lock()
	c.addObjectLocked(o)
	c.mu.Unlock()
}

// Provide registers the row provider of a provided table.
func (c *Catalog) Provide(id types.TableID, p types.RowProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.providers[id] = p
	if t, ok := c.tables[id]; ok {
		t.Kind = types.Provided
	}
}

// Provider returns the provider of a provided table.
func (c *Catalog) Provider(id types.TableID) (types.RowProvider, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.providers[id]
	return p, ok
}
