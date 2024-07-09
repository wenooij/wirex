package wirex

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/wenooij/wire"
)

func TestWritePrefixCompressSeqString(t *testing.T) {
	var b bytes.Buffer
	c := PrefixCompress(wire.RawString)
	if err := c.Write(&b, "a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(&b, "aa"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(&b, "aab"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(&b, "b"); err != nil {
		t.Fatal(err)
	}
	got := b.String()
	want := "\x02\x00a\x02\x01a\x02\x02b\x02\x00b"
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("TestWritePrefixCompressSeqString(): got diff:\n%s", diff)
	}
}
