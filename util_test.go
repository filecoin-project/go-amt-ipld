package amt

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

var width uint = 2

func BenchmarkNodesForHeight(b *testing.B) {
	width = 9
	for i := 0; i < b.N; i++ {
		nodesForHeight(width, i%15)
	}
}

func TestNodesForHeight(t *testing.T) {
	require.Equal(t, uint64(1), nodesForHeight(1, 0))
	require.Equal(t, uint64(4), nodesForHeight(2, 1))
	require.Equal(t, uint64(64), nodesForHeight(3, 2))
	require.Equal(t, uint64(4096), nodesForHeight(4, 3))
}

// A CBOR-marshalable byte array.
// Note: this is duplicated from the HAMT tests. We should extract common CBOR manipulation utilities to a
// Filecoin shared library.
type CborByteArray []byte

func (c *CborByteArray) MarshalCBOR(w io.Writer) error {
	if err := cbg.WriteMajorTypeHeader(w, cbg.MajByteString, uint64(len(*c))); err != nil {
		return err
	}
	_, err := w.Write(*c)
	return err
}

func (c *CborByteArray) UnmarshalCBOR(r io.Reader) error {
	maj, extra, err := cbg.CborReadHeader(r)
	if err != nil {
		return err
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	if uint64(cap(*c)) < extra {
		*c = make([]byte, extra)
	}
	if _, err := io.ReadFull(r, *c); err != nil {
		return err
	}
	return nil
}

func cborstr(s string) *CborByteArray {
	v := CborByteArray(s)
	return &v
}

func TestStringCborRoundtrip(t *testing.T) {
	input := "foo"

	val := cborstr(input)
	valueBuf := new(bytes.Buffer)
	if err := val.MarshalCBOR(valueBuf); err != nil {
		t.Fatal(err)
	}
	encoded := valueBuf.Bytes()

	var actual CborByteArray
	cbor.DecodeInto(encoded, &actual)

	if string(actual) != input {
		t.Errorf("got %s, wanted %s", actual, input)
	}
}
