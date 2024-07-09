package wirex

import (
	"bytes"

	"github.com/wenooij/wire"
)

var rawPrefixRecord = wire.Tup2(wire.Uvarint64, wire.Raw)

var prefixRecord = wire.Span(rawPrefixRecord)

// PrefixCompressProto is a stateful Proto for (usually sorted) prefix compressed records.
type PrefixCompressProto[E any] struct {
	proto wire.Proto[E]
	cur   bytes.Buffer
	last  bytes.Buffer
}

// PrefixCompress provides a proto for run-length encoded records.
//
// Best used on sorted data.
func PrefixCompress[E any](proto wire.Proto[E]) *PrefixCompressProto[E] {
	return &PrefixCompressProto[E]{proto: proto}
}

func (p PrefixCompressProto[E]) Read(r wire.Reader) (E, error) {
	var (
		prev bytes.Buffer
		b    bytes.Buffer
	)
	se, err := prefixRecord.Read(r)
	if err != nil {
		var e E
		return e, err
	}
	n := se.Elem().E0
	b.Reset()
	b.Write(prev.Bytes()[:n]) // prefix
	b.Write(se.Elem().E1)     // posfix
	prev.Reset()
	prev.Write(b.Bytes())
	elem, err := p.proto.Read(&b)
	if err != nil {
		var e E
		return e, err
	}
	// Call user handler.
	return elem, nil
}
func (p *PrefixCompressProto[E]) Write(w wire.Writer, record E) error {
	p.proto.Write(&p.cur, record)
	if err := p.flushRecord(w); err != nil {
		return err
	}
	return nil
}

// flushRecord should be called at the barrier between records
// so that the prefix compression algorithm can function.
func (p *PrefixCompressProto[E]) flushRecord(w wire.Writer) error {
	var n uint64
	cur, last := p.cur.Bytes(), p.last.Bytes()
	for i := 0; i < len(cur) && i < len(last) && cur[i] == last[i]; i++ {
		n++
	}
	if err := prefixRecord.Write(w, prefixRecord.Make(wire.Tup2Val[uint64, []byte]{E0: n, E1: cur[n:]})); err != nil {
		return err
	}
	p.last.Reset()
	p.last.ReadFrom(&p.cur)
	p.cur.Reset()
	return nil
}

func (p PrefixCompressProto[E]) Size(record E) uint64 { return p.proto.Size(record) }
