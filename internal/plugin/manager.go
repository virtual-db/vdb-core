// Package plugin manages plugin lifecycle and JSON-RPC dispatch.
package plugin

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/virtual-db/vdb-core/internal/framework"
)

// Manager discovers plugin manifests, launches subprocesses, and owns the
// collection of active plugin connections for the duration of the process.
type Manager struct {
	pluginDir string
	plugins   []*pluginInstance
	timeout   time.Duration
}

// pluginInstance represents one loaded plugin.
type pluginInstance struct {
	id       string
	manifest Manifest
	declare  DeclareParams
	cmd      *exec.Cmd
	conn     *pluginConn
	mu       sync.Mutex
	failed   bool
	done     chan struct{}
}

// NewManager creates a Manager that will scan pluginDir for plugins at ConnectAll time.
func NewManager(pluginDir string, timeout time.Duration) *Manager {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Manager{
		pluginDir: pluginDir,
		timeout:   timeout,
	}
}

// ConnectAll scans pluginDir, launches every plugin it finds, wires pipeline
// handler and event subscription adapters into bus and pipe, and starts each
// plugin's inbound reader goroutine.
func (m *Manager) ConnectAll(bus *framework.Bus, pipe *framework.Pipeline) {
	if m.pluginDir == "" {
		return
	}

	entries, err := os.ReadDir(m.pluginDir)
	if err != nil {
		slog.Warn(fmt.Sprintf("plugin: cannot read plugin directory %q: %v", m.pluginDir, err), "pkg", "plugin")
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirPath := filepath.Join(m.pluginDir, entry.Name())
		inst, rawConn, err := m.launchPlugin(dirPath)
		if errors.Is(err, errNoManifest) {
			continue
		}
		if err != nil {
			slog.Warn(fmt.Sprintf("plugin: failed to launch plugin in %q: %v", dirPath, err), "pkg", "plugin")
			continue
		}

		pConn, err := m.readDeclare(inst, rawConn, bus, pipe)
		if err != nil {
			slog.Warn(fmt.Sprintf("plugin: %q declare failed: %v; closing connection and killing process", inst.id, err), "pkg", "plugin")
			_ = rawConn.Close()
			_ = inst.cmd.Process.Kill()
			continue
		}

		inst.conn = pConn
		inst.done = make(chan struct{})
		m.plugins = append(m.plugins, inst)
		slog.Warn(fmt.Sprintf("plugin: %q connected (pid %d, version %s)",
			inst.id, inst.cmd.Process.Pid, inst.manifest.Version), "pkg", "plugin")
		go m.monitorPlugin(inst)
	}
}

func (m *Manager) monitorPlugin(inst *pluginInstance) {
	err := inst.cmd.Wait()

	inst.mu.Lock()
	alreadyFailed := inst.failed
	inst.failed = true
	pConn := inst.conn
	inst.mu.Unlock()

	if pConn != nil {
		_ = pConn.Close()
	}

	close(inst.done)

	if !alreadyFailed {
		if err != nil {
			slog.Warn(fmt.Sprintf("plugin %q (pid %d): exited unexpectedly: %v",
				inst.id, inst.cmd.Process.Pid, err), "pkg", "plugin")
		} else {
			slog.Warn(fmt.Sprintf("plugin %q (pid %d): exited unexpectedly with exit status 0",
				inst.id, inst.cmd.Process.Pid), "pkg", "plugin")
		}
	}
}

// Shutdown gracefully terminates all live plugins.
func (m *Manager) Shutdown() {
	var wg sync.WaitGroup
	for _, inst := range m.plugins {
		inst := inst

		inst.mu.Lock()
		failed := inst.failed
		pConn := inst.conn
		inst.mu.Unlock()

		if failed || pConn == nil {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			shutResult := make(chan error, 1)
			go func() { shutResult <- pConn.sendShutdown() }()

			select {
			case err := <-shutResult:
				if err != nil {
					slog.Warn(fmt.Sprintf("plugin %q: shutdown request failed: %v; killing process", inst.id, err), "pkg", "plugin")
					_ = inst.cmd.Process.Kill()
				}
			case <-time.After(m.timeout):
				slog.Warn(fmt.Sprintf("plugin %q: shutdown timed out after %v; killing process",
					inst.id, m.timeout), "pkg", "plugin")
				_ = inst.cmd.Process.Kill()
			}

			select {
			case <-inst.done:
			case <-time.After(m.timeout):
				slog.Warn(fmt.Sprintf("plugin %q: process did not exit after kill; sending SIGKILL again", inst.id), "pkg", "plugin")
				_ = inst.cmd.Process.Kill()
				select {
				case <-inst.done:
				case <-time.After(2 * time.Second):
					slog.Warn(fmt.Sprintf("plugin %q: process still alive after second kill; giving up", inst.id), "pkg", "plugin")
				}
			}
		}()
	}
	wg.Wait()
}

// Transforms returns every column a connected plugin declared it transforms,
// as "database.table.column".
func (m *Manager) Transforms() []string {
	var out []string
	for _, inst := range m.plugins {
		inst.mu.Lock()
		out = append(out, inst.declare.Transforms...)
		inst.mu.Unlock()
	}
	return out
}
