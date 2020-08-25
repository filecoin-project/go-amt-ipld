package fuzzer

import (
	"encoding/binary"
	"fmt"

	"github.com/filecoin-project/go-amt-ipld/v2"
)

var Debug = false

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
	code       opCode
	key, value uint64
}

func Parse(data []byte) (ops []op) {
	scratch := make([]byte, 17)

	for len(data) > 0 {
		n := copy(scratch, data)
		data = data[n:]

		code := opCode(scratch[0] % byte(opMax))
		k := binary.LittleEndian.Uint64(scratch[1:]) % amt.MaxIndex
		v := binary.LittleEndian.Uint64(scratch[9:])
		ops = append(ops, op{code, k, v})
	}
	return ops
}

func Fuzz(data []byte) int {
	if len(data) < 1 {
		return -1
	}

	arr := newCheckedAMT()
	for _, op := range Parse(data) {
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
	if Debug {
		fmt.Printf("checking\n")
	}
	arr.check()
	return 0
}
