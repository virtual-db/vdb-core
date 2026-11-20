package framework

import (
	"sync"

	"github.com/AnqorDX/pipeline"
)

// PointFunc is the handler type for all pipeline point handlers.
// It is a type alias for pipeline.HandlerFunc, which is:
//
//	func(ctx any, payload any) (any, any, error)
type PointFunc = pipeline.HandlerFunc

// Registrar is the interface each domain Handlers struct accepts for
// registration. *Pipeline satisfies it. Tests can satisfy it with a stub.
type Registrar interface {
	Attach(point string, priority int, fn PointFunc) error
}

// Pipeline is the VDB Core pipeline abstraction. It is the sole owner of the
// underlying pipeline.Registry and the only type in the framework that interacts
// with the pipeline library directly. The zero value is safe: Process returns
// (nil, nil) when reg is nil.
type Pipeline struct {
	reg    *pipeline.Registry
	global *GlobalContext
	ext    *attached
}

// attached counts handlers attached by anything other than the built-in groups.
type attached struct {
	mu sync.Mutex
	n  map[string]int
}

// NewPipeline constructs a Pipeline. global must be a pointer to the App's global
// field so the wrapper always reads the post-seal GlobalContext value.
func NewPipeline(global *GlobalContext) *Pipeline {
	return &Pipeline{reg: pipeline.NewRegistry(), global: global, ext: &attached{n: map[string]int{}}}
}

// DeclarePipeline registers a named pipeline with its ordered point sequence on
// the underlying registry.
func (p *Pipeline) DeclarePipeline(name string, points []string) {
	p.reg.DeclarePipeline(name, points)
}

// Attach attaches fn to point at priority. Returns an error if the point
// does not exist.
func (p *Pipeline) Attach(point string, priority int, fn PointFunc) error {
	if err := p.reg.Register(point, priority, fn); err != nil {
		return err
	}
	p.ext.mu.Lock()
	p.ext.n[point]++
	p.ext.mu.Unlock()
	return nil
}

// HasHandlers reports whether anything other than a built-in handler is
// attached to point.
func (p *Pipeline) HasHandlers(point string) bool {
	if p.ext == nil {
		return false
	}
	p.ext.mu.Lock()
	defer p.ext.mu.Unlock()
	return p.ext.n[point] > 0
}

// Builtin returns a Registrar for the built-in handler groups: their
// handlers are not counted by HasHandlers.
func (p *Pipeline) Builtin() Registrar { return builtin{p} }

type builtin struct{ p *Pipeline }

func (b builtin) Attach(point string, priority int, fn PointFunc) error {
	return b.p.reg.Register(point, priority, fn)
}

// MustAttach is like Attach but panics on error, indicating a framework
// programming error (e.g. DeclarePipeline was not called before Attach).
func (p *Pipeline) MustAttach(point string, priority int, fn PointFunc) {
	if err := p.reg.Register(point, priority, fn); err != nil {
		panic("framework: " + err.Error())
	}
}

// Process runs the named pipeline.
// ctx is optional. When provided, it is used as the starting HandlerContext.
// When omitted, a HandlerContext carrying the current sealed GlobalContext is injected.
// Returns (nil, nil) when reg is nil (zero-value safety).
func (p Pipeline) Process(name string, payload any, ctx ...HandlerContext) (any, error) {
	if p.reg == nil {
		return nil, nil
	}
	var runCtx HandlerContext
	if p.global != nil {
		runCtx = HandlerContext{Global: *p.global}
	}
	if len(ctx) > 0 {
		runCtx = ctx[0]
	}
	return p.reg.Process(name, runCtx, payload)
}

// BuildContext is a ready-made PointFunc that stamps a fresh CorrelationID
// onto HandlerContext. Register it at every *.build_context point.
func BuildContext(ctx any, p any) (any, any, error) {
	hctx := ctx.(HandlerContext)
	hctx.CorrelationID = NewCorrelationID(hctx.CorrelationID)
	return hctx, p, nil
}
