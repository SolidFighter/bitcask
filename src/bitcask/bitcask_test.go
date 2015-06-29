package bitcask

import (
	//"math/rand"
	"os"
	"testing"
	"fmt"
)

const (
	testDirPath     = "/home/myang/testData"
	G           int = 1024 * 1024 * 1024
	M           int = 1024 * 1024
	K           int = 1024
)

var options Options = Options{
	MaxFileSize:  int32(G),
	//MergeWindow:  [2]int{0, 23},
	MergeTrigger: 0.6,
	Path:         testDirPath,
}


func TestNewBitcask(t *testing.T) {
	b, err := NewBitcask(options)
	defer os.RemoveAll(b.Path)
	if err != nil {
		t.Errorf("Error:%s, while opening directory %s.", err.Error(), options.Path)
	}
	err = b.Close()
	if err != nil {
		t.Errorf("Error %s while closing bitcask.", err.Error())
	}
}

type TestKeyValue struct {
	key      string
	value    []byte
}

var Testdata = []TestKeyValue{
	TestKeyValue{"key1", []byte("value1")},
	TestKeyValue{"key2", []byte("value2")},
	TestKeyValue{"key3", []byte("value3")},
	TestKeyValue{"key4", []byte("value4")},
}


func TestBcOpe(t *testing.T) {
	b, _ := NewBitcask(options)
	defer os.RemoveAll(b.Path)
	for _, kv := range Testdata {
		err := b.Set(kv.key, kv.value)
		if err != nil {
			t.Fatalf("Error %s while Seting %s.", err.Error(), kv.key)
		}
	}

	for k, v := range b.kd.kv {
		fmt.Printf("key = %s, fid = %d, timeStamp = %d, valueSize = %d, valuePos = %d.\n", k, v.fid, v.timeStamp, v.valueSize, v.valuePos)
	}

	b.Close()

	b, _ = NewBitcask(options)
	for k, v := range b.kd.kv {
		fmt.Printf("key = %s, fid = %d, timeStamp = %d, valueSize = %d, valuePos = %d.\n", k, v.fid, v.timeStamp, v.valueSize, v.valuePos)
	}

	for _, kv := range Testdata {
		v, err := b.Get(kv.key)
		if err != nil {
			t.Fatalf("Error %s while Geting %s", err.Error(), kv.key)
		}
		if string(v) != string(kv.value) {
			t.Fatalf("Exptected %s, got %s", string(kv.value), string(v))
		}
	}

	keys := b.Keys()
	for key := range keys {
		fmt.Printf("key = %s.\n", key)
	}

	b.Del(Testdata[0].key)
	b.Close()
	b, _ = NewBitcask(options)
	for k, v := range b.kd.kv {
		fmt.Printf("key = %s, fid = %d, timeStamp = %d, valueSize = %d, valuePos = %d.\n", k, v.fid, v.timeStamp, v.valueSize, v.valuePos)
	}
}


func BenchmarkSet100B(t *testing.B) {
	benchSet(t, 100)
}


func BenchmarkSet1K(t *testing.B) {
	benchSet(t, K)
}


func BenchmarkSet1M(t *testing.B) {
	benchSet(t, M)
}


func BenchmarkSet10M(t *testing.B) {
	benchSet(t, 10 * M)
}


func benchSet(t *testing.B, size int) {
	t.StopTimer()
	os.RemoveAll(testDirPath)
	b, _ := NewBitcask(options)
	value := genValue(size)
	t.SetBytes(int64(size))

	t.StartTimer()
	for i := 0; i < t.N; i++ {
		b.Set(string(i), value)
		//b.Sync()
	}
	b.Close()
}

/*


func BenchmarkGet1K(t *testing.B) {
	benchGet(t, K)
}

func BenchmarkGet1M(t *testing.B) {
	benchGet(t, M)
}

func benchGet(t *testing.B, size int) {
	b, _ := NewBitcask(O)
	t.SetBytes(int64(size))
	t.StartTimer()
	for i := 0; i < t.N; i++ {
		b.Get(string(i))
	}
	t.StopTimer()
	b.Close()
}
*/
