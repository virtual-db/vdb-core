package lock

import (
	"bytes"

	"github.com/virtual-db/vdb-core/internal/types"
)

// bound is one end of a half-open byte interval [lo, hi) in plain
// lexicographic order. inf marks an unbounded high end.
type bound struct {
	b   string
	inf bool
}

func lessHi(a, b bound) bool { // a < b, both high ends
	if a.inf {
		return false
	}
	if b.inf {
		return true
	}
	return a.b < b.b
}

// span returns the interval a resource is indexed under. For a range it is
// [Low, past High): the low end ignores LowIncl, so the interval is a superset
// of the range, and two spans overlap exactly when rangesOverlap says so.
// Callers filter candidates with overlap.
func span(r types.Resource) (lo string, hi bound) {
	switch {
	case r.Whole:
		return "", bound{inf: true}
	case isPoint(r):
		return string(r.Point), bound{b: string(r.Point) + "\x00"}
	}
	lo = string(r.Range.Low)
	switch {
	case len(r.Range.High) == 0:
		hi = bound{inf: true}
	case r.Range.HighIncl:
		if s := successor(r.Range.High); s == nil {
			hi = bound{inf: true}
		} else {
			hi = bound{b: string(s)}
		}
	default:
		hi = bound{b: string(r.Range.High)}
	}
	return lo, hi
}

// successor returns the smallest byte string greater than every string with
// prefix p, or nil when there is none.
func successor(p []byte) []byte {
	s := append([]byte(nil), p...)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != 0xFF {
			s[i]++
			return s[:i+1]
		}
	}
	return nil
}

// itree is an interval treap of lock entries keyed by (lo, seq), each node
// carrying the largest high end in its subtree, so that finding the entries
// that overlap an interval costs O(log n + matches).
type itree struct {
	root *inode
	n    int
	rnd  uint64
}

type inode struct {
	lo          string
	hi, max     bound
	e           *entry
	pri         uint64
	left, right *inode
}

func (t *itree) next() uint64 {
	// xorshift64*: deterministic, cheap; priorities only need to look random.
	if t.rnd == 0 {
		t.rnd = 0x9E3779B97F4A7C15
	}
	t.rnd ^= t.rnd >> 12
	t.rnd ^= t.rnd << 25
	t.rnd ^= t.rnd >> 27
	return t.rnd * 2685821657736338717
}

func keyLess(lo string, seq uint64, n *inode) bool {
	if lo != n.lo {
		return lo < n.lo
	}
	return seq < n.e.seq
}

func (n *inode) fix() {
	n.max = n.hi
	if n.left != nil && lessHi(n.max, n.left.max) {
		n.max = n.left.max
	}
	if n.right != nil && lessHi(n.max, n.right.max) {
		n.max = n.right.max
	}
}

func rotRight(n *inode) *inode {
	l := n.left
	n.left = l.right
	l.right = n
	n.fix()
	l.fix()
	return l
}

func rotLeft(n *inode) *inode {
	r := n.right
	n.right = r.left
	r.left = n
	n.fix()
	r.fix()
	return r
}

func (t *itree) insert(e *entry) {
	lo, hi := span(e.res)
	nn := &inode{lo: lo, hi: hi, max: hi, e: e, pri: t.next()}
	t.root = insertNode(t.root, nn)
	t.n++
}

func insertNode(n, nn *inode) *inode {
	if n == nil {
		return nn
	}
	if keyLess(nn.lo, nn.e.seq, n) {
		n.left = insertNode(n.left, nn)
		if n.left.pri > n.pri {
			n = rotRight(n)
		}
	} else {
		n.right = insertNode(n.right, nn)
		if n.right.pri > n.pri {
			n = rotLeft(n)
		}
	}
	n.fix()
	return n
}

func (t *itree) remove(e *entry) {
	lo, _ := span(e.res)
	var ok bool
	t.root, ok = removeNode(t.root, lo, e)
	if ok {
		t.n--
	}
}

func removeNode(n *inode, lo string, e *entry) (*inode, bool) {
	if n == nil {
		return nil, false
	}
	var ok bool
	switch {
	case n.e == e:
		return merge(n.left, n.right), true
	case keyLess(lo, e.seq, n):
		n.left, ok = removeNode(n.left, lo, e)
	default:
		n.right, ok = removeNode(n.right, lo, e)
	}
	n.fix()
	return n, ok
}

func merge(a, b *inode) *inode {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.pri > b.pri {
		a.right = merge(a.right, b)
		a.fix()
		return a
	}
	b.left = merge(a, b.left)
	b.fix()
	return b
}

// overlapping calls fn for every entry whose span overlaps [lo, hi), until fn
// returns false.
func (t *itree) overlapping(lo string, hi bound, fn func(*entry) bool) {
	var walk func(n *inode) bool
	walk = func(n *inode) bool {
		if n == nil {
			return true
		}
		// Nothing in this subtree ends after lo.
		if !lessHi(bound{b: lo}, n.max) {
			return true
		}
		if !walk(n.left) {
			return false
		}
		// This node and everything right of it start at or after hi.
		if !hi.inf && n.lo >= hi.b {
			return true
		}
		if lessHi(bound{b: lo}, n.hi) && !fn(n.e) {
			return false
		}
		return walk(n.right)
	}
	walk(t.root)
}

// bytesKey is a resource's identity within its slot, for exact-match maps.
func bytesKey(r types.Resource) string {
	switch {
	case r.Whole:
		return "w"
	case isPoint(r):
		return "p" + string(r.Point)
	}
	var b bytes.Buffer
	b.WriteByte('r')
	flags := byte(0)
	if r.Range.LowIncl {
		flags |= 1
	}
	if r.Range.HighIncl {
		flags |= 2
	}
	b.WriteByte(flags)
	writeLen(&b, len(r.Range.Low))
	b.Write(r.Range.Low)
	b.Write(r.Range.High)
	return b.String()
}

func writeLen(b *bytes.Buffer, n int) {
	for n >= 0x80 {
		b.WriteByte(byte(n) | 0x80)
		n >>= 7
	}
	b.WriteByte(byte(n))
}
