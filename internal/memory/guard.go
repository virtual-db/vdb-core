// Package memory implements the memory rule (implementation plan doc 06 §4,
// product doc 12 §4): it reads the memory available to the container, keeps a
// reserve free, and refuses a growing write or a new lock entry that would
// cross the ceiling.
package memory

import (
	"bufio"
	"bytes"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/virtual-db/vdb-core/internal/types"
)

// Paths are the files the guard reads; tests replace them with fixtures.
type Paths struct {
	CgroupV2Max string
	CgroupV1Max string
	MemInfo     string
}

// DefaultPaths are the Linux locations.
var DefaultPaths = Paths{
	CgroupV2Max: "/sys/fs/cgroup/memory.max",
	CgroupV1Max: "/sys/fs/cgroup/memory/memory.limit_in_bytes",
	MemInfo:     "/proc/meminfo",
}

// Config configures the guard.
type Config struct {
	Reserve       float64 // fraction kept free; 0.10 by default
	DeltaMaxBytes uint64  // 0 means no fixed cap
	Probe         types.MemoryProbe
	DeltaBytes    func() uint64 // the Delta's accounted size, for the fixed cap
	Paths         Paths
	OnRefusal     func(kind string, limit uint64)
}

// Guard decides whether memory may grow. Safe for concurrent use.
type Guard struct {
	cfg       Config
	available uint64
	ceiling   uint64

	mu        sync.Mutex
	lastRead  time.Time
	lastRSS   uint64
	sinceRead int64

	refusals atomic.Uint64
	now      func() time.Time
}

// SetProbe installs the probe of every process to count. Called once, before
// the first write (UseDriver).
func (g *Guard) SetProbe(p types.MemoryProbe) {
	g.mu.Lock()
	g.cfg.Probe = p
	g.mu.Unlock()
}

// New builds a guard and sets the Go runtime's memory limit to the ceiling.
func New(cfg Config) *Guard {
	if cfg.Reserve <= 0 || cfg.Reserve >= 1 {
		cfg.Reserve = 0.10
	}
	if cfg.Paths == (Paths{}) {
		cfg.Paths = DefaultPaths
	}
	g := &Guard{cfg: cfg, now: time.Now}
	g.available = Available(cfg.Paths)
	if g.available > 0 {
		g.ceiling = uint64(float64(g.available) * (1 - cfg.Reserve))
		debug.SetMemoryLimit(int64(g.ceiling))
	}
	return g
}

// SetClock replaces the clock (tests).
func (g *Guard) SetClock(now func() time.Time) { g.now = now }

// Available reads the memory available: cgroup v2, then cgroup v1, then MemTotal.
func Available(p Paths) uint64 {
	if b, err := os.ReadFile(p.CgroupV2Max); err == nil {
		s := strings.TrimSpace(string(b))
		if s != "max" {
			if n, err := strconv.ParseUint(s, 10, 64); err == nil && n > 0 {
				return n
			}
		}
	}
	if b, err := os.ReadFile(p.CgroupV1Max); err == nil {
		if n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil && n > 0 && n < 1<<62 {
			return n
		}
	}
	if b, err := os.ReadFile(p.MemInfo); err == nil {
		sc := bufio.NewScanner(bytes.NewReader(b))
		for sc.Scan() {
			f := strings.Fields(sc.Text())
			if len(f) >= 2 && f[0] == "MemTotal:" {
				if n, err := strconv.ParseUint(f[1], 10, 64); err == nil {
					return n * 1024
				}
			}
		}
	}
	return 0
}

// Reserve asks for room for n more bytes. kind names what grows ("delta" or
// "lock") for the refusal event. It returns nil, or the refusal reason.
func (g *Guard) Reserve(n int, kind string) error {
	if g == nil {
		return nil
	}
	if g.cfg.DeltaMaxBytes > 0 && kind == "delta" && g.cfg.DeltaBytes != nil {
		if g.cfg.DeltaBytes()+uint64(n) > g.cfg.DeltaMaxBytes {
			return g.refuse(kind, g.cfg.DeltaMaxBytes)
		}
	}
	if g.ceiling == 0 || g.cfg.Probe == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.now().Sub(g.lastRead) > 10*time.Millisecond {
		rss, err := g.cfg.Probe()
		if err == nil {
			g.lastRSS = rss
			g.sinceRead = 0
			g.lastRead = g.now()
		}
	}
	g.sinceRead += int64(n)
	if g.lastRSS+uint64(g.sinceRead) > g.ceiling {
		g.sinceRead -= int64(n)
		return g.refuse(kind, g.ceiling)
	}
	return nil
}

func (g *Guard) refuse(kind string, limit uint64) error {
	g.refusals.Add(1)
	if g.cfg.OnRefusal != nil {
		g.cfg.OnRefusal(kind, limit)
	}
	code := types.ErrDeltaFull
	if kind == "lock" {
		code = types.ErrLockMemory
	}
	return &types.Error{Code: code, Msg: "the " + kind + " memory limit of " + strconv.FormatUint(limit, 10) + " bytes is reached (VDB_MEMORY_RESERVE, VDB_DELTA_MAX_BYTES)"}
}

// Stats returns the guard's readings.
func (g *Guard) Stats() types.MemoryStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return types.MemoryStats{Available: g.available, Ceiling: g.ceiling, Resident: g.lastRSS, Reserve: g.cfg.Reserve, Refusals: g.refusals.Load()}
}
