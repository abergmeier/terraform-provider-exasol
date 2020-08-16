package internal

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"reflect"
)

func HashStrings(elems ...string) ([]byte, error) {

	sha := sha256.New()

	for _, elem := range elems {
		_, err := sha.Write([]byte(elem))
		if err != nil {
			return nil, err
		}
	}

	return sha.Sum(nil), nil
}

func HashUnknown(elems ...[]interface{}) ([]byte, error) {

	sha := sha256.New()

	for _, elem := range elems {
		for _, f := range elem {
			if f == nil {
				continue
			}
			t := reflect.TypeOf(f)
			switch t.Kind() {
			case reflect.String:
				_, err := sha.Write([]byte(f.(string)))
				if err != nil {
					return nil, err
				}
			case reflect.Float64:
				var buf [8]byte
				binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f.(float64)))
				_, err := sha.Write(buf[:])
				if err != nil {
					return nil, err
				}
			case reflect.Bool:
				var b [1]byte
				if f.(bool) {
					b[0] = 1
				} else {
					b[0] = 0
				}
				_, err := sha.Write(b[:])
				if err != nil {
					return nil, err
				}
			default:
				panic(t)
			}
		}
	}

	return sha.Sum(nil), nil
}
