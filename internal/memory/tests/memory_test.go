// Package memory_test — tests/memory_test.go
//
// cgroup v2, v1 and no-cgroup readings from fixture files; reserve and
// fixed-cap refusals; probe refresh (doc 06 §6).
package memory_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/virtual-db/vdb-core/internal/memory"
)

func fixture(t *testing.T, v2, v1, meminfo string) Paths {
	d := t.TempDir()
	p := Paths{CgroupV2Max: filepath.Join(d, "v2"), CgroupV1Max: filepath.Join(d, "v1"), MemInfo: filepath.Join(d, "meminfo")}
	for path, body := range map[string]string{p.CgroupV2Max: v2, p.CgroupV1Max: v1, p.MemInfo: meminfo} {
		if body != "" {
			os.WriteFile(path, []byte(body), 0o644)
		}
	}
	return p
}

func TestAvailable(t *testing.T) {
	if n := Available(fixture(t, "1073741824\n", "", "")); n != 1<<30 {
		t.Fatalf("v2: %d", n)
	}
	if n := Available(fixture(t, "max\n", "536870912\n", "")); n != 512<<20 {
		t.Fatalf("v1: %d", n)
	}
	if n := Available(fixture(t, "", "", "MemTotal:  2048 kB\n")); n != 2<<20 {
		t.Fatalf("meminfo: %d", n)
	}
}

func TestReserve(t *testing.T) {
	rss := uint64(800 << 20)
	g := New(Config{Reserve: 0.10, Probe: func() (uint64, error) { return rss, nil }, Paths: fixture(t, "1073741824", "", "")})
	now := time.Unix(0, 0)
	g.SetClock(func() time.Time { return now })
	if err := g.Reserve(50<<20, "delta"); err != nil {
		t.Fatalf("under the ceiling: %v", err)
	}
	if err := g.Reserve(200<<20, "delta"); err == nil {
		t.Fatal("over the ceiling must be refused")
	}
	capped := New(Config{DeltaMaxBytes: 100, DeltaBytes: func() uint64 { return 90 }, Paths: fixture(t, "", "", "")})
	if capped.Reserve(20, "delta") == nil {
		t.Fatal("the fixed cap must refuse")
	}
}
