package utils

import (
	"encoding/binary"
)

const (
	BigEndian = iota
	LittleEndian
)

func ByteToUnint16(b []byte, bl int) []uint16 {
	l := len(b) / 2
	res := make([]uint16, l, l)
	for i := 0; i < l; i++ {
		if BigEndian == bl {
			res[i] = binary.BigEndian.Uint16(b[2*i : 2*i+2])
		}
		if LittleEndian == bl {
			res[i] = binary.LittleEndian.Uint16(b[2*i : 2*i+2])
		}
	}
	return res
}

func ByteToUnint32(b []byte, bl int) []uint32 {
	l := len(b) / 4
	res := make([]uint32, l, l)
	for i := 0; i < l; i++ {
		if BigEndian == bl {
			res[i] = binary.BigEndian.Uint32(b[4*i : 4*i+4])
		}
		if LittleEndian == bl {
			res[i] = binary.LittleEndian.Uint32(b[4*i : 4*i+4])
		}
	}
	return res
}

func ByteToUnint16B(b []byte) []uint16 {
	return ByteToUnint16(b, BigEndian)
}

func ByteToUnint16L(b []byte) []uint16 {
	return ByteToUnint16(b, LittleEndian)
}

func ByteToUnint32B(b []byte) []uint32 {
	return ByteToUnint32(b, BigEndian)
}

func ByteToUnint32L(b []byte) []uint32 {
	return ByteToUnint32(b, LittleEndian)
}

func Uint16ToBytes(values []uint16, bl int) []byte {
	bytes := make([]byte, len(values)*2)

	for i, value := range values {
		if BigEndian == bl {
			binary.BigEndian.PutUint16(bytes[i*2:(i+1)*2], value)
		}
		if LittleEndian == bl {
			binary.LittleEndian.PutUint16(bytes[i*2:(i+1)*2], value)
		}
	}
	return bytes
}

func Uint16ToBytesB(values []uint16, bl int) []byte {
	return Uint16ToBytes(values, BigEndian)
}

func Uint16ToBytesL(values []uint16, bl int) []byte {
	return Uint16ToBytes(values, LittleEndian)
}

func Uint32ToBytes(values []uint32, bl int) []byte {
	bytes := make([]byte, len(values)*4)

	for i, value := range values {
		if BigEndian == bl {
			binary.BigEndian.PutUint32(bytes[i*4:(i+1)*4], value)
		}
		if LittleEndian == bl {
			binary.LittleEndian.PutUint32(bytes[i*4:(i+1)*4], value)
		}

	}
	return bytes
}

func Uint32ToBytesB(values []uint32) []byte {
	return Uint32ToBytes(values, BigEndian)
}
func Uint32ToBytesL(values []uint32) []byte {
	return Uint32ToBytes(values, LittleEndian)
}
