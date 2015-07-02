package bitcask

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const (
	testDirPath     = "/home/myang/testData"
	G           int = 1024 * 1024 * 1024
	M           int = 1024 * 1024
	K           int = 1024
)

var options Options = Options{
	//MaxFileSize: int32(G),
	MaxFileSize: 70,
	StartTime: 0,
	EndTime: 23,
	MergeTrigger: 0.6,
	Path:         testDirPath,
	IsCompress:   false,
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
	key   string
	value []byte
}

var Testdata = []TestKeyValue{
	TestKeyValue{"key1", []byte("value1")},
	TestKeyValue{"key2", []byte("value2")},
	TestKeyValue{"key3", []byte("value3")},
	TestKeyValue{"key4", []byte("value4")},
	TestKeyValue{"key5", []byte("value5")},
	TestKeyValue{"key6", []byte("value6")},
	TestKeyValue{"key7", []byte("value7")},
	TestKeyValue{"key8", []byte("value8")},
	TestKeyValue{"key9", []byte("value9")},
	TestKeyValue{"key10", []byte("value10")},
}

/*
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
*/


func TestMerge(t *testing.T) {
	fmt.Println("in TestMerge.")
	b, _ := NewBitcask(options)
	for _, kv := range Testdata {
		err := b.Set(kv.key, kv.value)
		if err != nil {
			t.Fatalf("Error %s while Seting %s.", err.Error(), kv.key)
		}
	}

	for i := 0; i < 8; i++ {
		b.Del(Testdata[i].key)
	}

	fmt.Printf("totalKeys = %d, deadKeys = %d.\n", b.totalKeys, b.deadKeys)

	//time.Sleep(30 * time.Second)

	/*
	for i := 0; i < K; i++ {
		b.Set(string(i), []byte(i))
	}
	*/

	time.Sleep(2 * time.Minute)

	keys := b.Keys()
	for key := range keys {
		value, _ := b.Get(key)
		fmt.Printf("key = %s, value = %s.\n", key, string(value))
	}

	fmt.Printf("totalKeys = %d, deadKeys = %d.\n", b.totalKeys, b.deadKeys)
	b.Close()
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
	benchSet(t, 10*M)
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

func BenchmarkGet100B(t *testing.B) {
	benchGet(t, 100)
}

func BenchmarkGet1K(t *testing.B) {
	benchGet(t, K)
}

func BenchmarkGet1M(t *testing.B) {
	benchGet(t, M)
}

func BenchmarkGet10M(t *testing.B) {
	benchGet(t, 10*M)
}

func benchGet(t *testing.B, size int) {
	t.StopTimer()
	//insert value before get
	os.RemoveAll(testDirPath)
	b, _ := NewBitcask(options)
	value := genValue(size)
	for i := 0; i < t.N; i++ {
		b.Set(string(i), value)
	}
	b.Close()

	b, _ = NewBitcask(options)
	t.SetBytes(int64(size))
	t.StartTimer()
	for i := 0; i < t.N; i++ {
		_, err := b.Get(string(i))
		if err != nil {
			t.Fatalf("%s.", err.Error())
		}
	}
	b.Close()
}

/*
func TestCompress(t *testing.T) {
	var value []byte
	value = genValue(K)
	fmt.Printf("value = %s, len = %d.\n", string(value), len(value))
	valuecom, _ := compress(value)
	fmt.Printf("valuecom = %s, len = %d.\n", string(valuecom), len(valuecom))
	value1, _ := uncompress(valuecom)
	fmt.Printf("value1 = %s, len = %d.\n", string(value1), len(value1))
	if string(value) != string(value1) {
		t.Errorf("uncompress failed.")
	}
}
*/

func BenchmarkCompress(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var value []byte
		value = genValue(MB)
		compress(value)
	}
}
