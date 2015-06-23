package bitcask



import (
//	"os"
	"testing"
	"fmt"
	"reflect"
	"math/rand"
)

const MB = 1024 * 1024

func TestRecordCompress(t *testing.T) {
	r := Record{123, 123, 5, 21, []byte("12345"), []byte("abcabcabcabcabcabcabc")}
	fmt.Println("crc = ", r.crc, " tstamp = ", r.tstamp, " ksz = ", r.ksz, "vsz = ", r.vsz, "key = ", r.key, " value = ", r.value)
	r.compress()
	fmt.Println("crc = ", r.crc, " tstamp = ", r.tstamp, " ksz = ", r.ksz, "vsz = ", r.vsz, "key = ", r.key, " value = ", r.value)
	r.uncompress()
	if r.crc != 123 || r.ksz != 5 || r.vsz != 21 || !reflect.DeepEqual(r.key, []byte("12345")) || !reflect.DeepEqual(r.value, []byte("abcabcabcabcabcabcabc")) {
		t.Errorf("compress failed.")
	}
}


func BenchmarkRecordCompress(b *testing.B) {
	b.StopTimer()
	v := genValue(MB)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		r := Record{123, 123, int32(len([]byte("abc"))), int32(len(v)), []byte("abc"), v}
		r.compress()
	}
}



/*
const testFilePath = "/tmp/1"

func TestBasic(t *testing.T) {
	//defer os.Remove(testFilePath)
	f, _ := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0666)
	b := newFile(f, 1)
	for _, kv := range Testdata {
		_, err := b.write(kv.key, kv.value, 0)
		if err != nil {
			t.Fatalf("Error %s while writing %s", err.Error(), kv.key)
		}
	}
	b.sync()
	b.io.Seek(0, 0)
	for _, kv := range Testdata {
		r, err := b.read()
		if err != nil {
			t.Fatalf("Error %s while reading %s", err.Error(), kv.key)
		}
		if string(r.key) != kv.key {
			t.Fatalf("Exptected %s, got %s", kv.key, string(r.key))
		}
		if string(r.value) != string(kv.value) {
			t.Fatalf("Exptected %s, got %s", string(kv.value), string(r.value))
		}
	}
}
*/


//Inner func
func genValue(size int) []byte {
	v := make([]byte, size)
	for i := 0; i < size; i++ {
		v[i] = uint8((rand.Int() % 26) + 97) // a-z
	}
	return v
}
