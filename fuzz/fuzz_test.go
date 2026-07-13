package fuzzer

import (
	"encoding/binary"
	"testing"

	"github.com/filecoin-project/go-amt-ipld/v4"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type opCode byte

const (
	opSet opCode = iota
	opSetSeen
	opGet
	opGetSeen
	opDelete
	opDeleteSeen
	opFlush
	opReload
	opMax
)

type op struct {
	code  opCode
	key   uint64
	value cbg.CborInt
}

func parseOps(data []byte) (ops []op) {
	scratch := make([]byte, 17)

	for len(data) > 0 {
		n := copy(scratch, data)
		data = data[n:]

		code := opCode(scratch[0] % byte(opMax))
		k := binary.LittleEndian.Uint64(scratch[1:]) % amt.MaxIndex
		v := binary.LittleEndian.Uint64(scratch[9:])
		ops = append(ops, op{code, k, cbg.CborInt(v)})
	}
	return ops
}

func encodeOp(code opCode, key, value uint64) []byte {
	buf := make([]byte, 17)
	buf[0] = byte(code)
	binary.LittleEndian.PutUint64(buf[1:], key)
	binary.LittleEndian.PutUint64(buf[9:], value)
	return buf
}

func seed(ops ...[]byte) (data []byte) {
	for _, op := range ops {
		data = append(data, op...)
	}
	return data
}

func FuzzAMTOps(f *testing.F) {
	f.Add(seed(
		encodeOp(opSet, 0, 100),
		encodeOp(opSet, 1, 101),
		encodeOp(opGet, 0, 0),
		encodeOp(opFlush, 0, 0),
		encodeOp(opDelete, 1, 0),
	))
	f.Add(seed(
		encodeOp(opSet, amt.MaxIndex-1, 1),
		encodeOp(opReload, 0, 0),
		encodeOp(opGetSeen, 3, 0),
		encodeOp(opDeleteSeen, 7, 0),
		encodeOp(opSetSeen, 2, 42),
	))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 1 {
			return
		}

		arr, err := newCheckedAMT(t)
		if err != nil {
			t.Fatal("failed to construct AMT:", err)
		}
		for _, op := range parseOps(data) {
			switch op.code {
			case opSet:
				arr.set(op.key, op.value)
			case opSetSeen:
				arr.setSeen(op.key, op.value)
			case opGet:
				arr.get(op.key)
			case opGetSeen:
				arr.getSeen(op.key)
			case opDelete:
				arr.delete(op.key)
			case opDeleteSeen:
				arr.deleteSeen(op.key)
			case opFlush:
				arr.flush()
			case opReload:
				arr.reload()
			default:
				panic("impossible")
			}
		}
		arr.check()
	})
}
