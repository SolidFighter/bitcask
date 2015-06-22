package bitcask

import (
	"testing"
	"fmt"
)

//go test -test.bench=".*"
//go test -test.bench="test func name"

func TestKeyDirBasicOp(t *testing.T) {
	kd := newKeyDir()
	kd.add("fvck", 1, 1, 1, 1000)
	kd.add("fvck1", 1, 1, 1, 1001)
	it, _ := kd.get("fvck")
	if it.fid != 1 && it.timeStamp != 1000 && it.valuePos != 1 && it.valueSize != 1{
		t.Errorf("get(fvck) failed.")
	}
	it, _ = kd.get("fvck1")
	if it.fid != 1 && it.timeStamp != 1001 && it.valuePos != 1 && it.valueSize != 1{
		t.Errorf("get(fvck1) failed.")
	}

	keysChan := kd.keys()
	for key := range keysChan {
		fmt.Println("key:", key)
	}

	kd.delete("fvck")
	var ok bool
	it, ok = kd.get("fvck")
	if ok {
		t.Errorf("delete(fvck) failed.")
	}
}


func BenchmarkKeyDirAdd(b *testing.B) {
	b.StopTimer()
	kd := newKeyDir()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		kd.add(string(i), 1, 1, 1, 1000)
	}
}


func BenchmarkKeyDirGet(b *testing.B) {
	b.StopTimer()
	kd := newKeyDir()
	for i := 0; i < b.N; i++ {
		kd.add(string(i), 1, 1, 1, 1000)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		kd.get(string(i))
	}
}


func BenchmarkKeyDirDelete(b *testing.B) {
	b.StopTimer()
	kd := newKeyDir()
	for i := 0; i < b.N; i++ {
		kd.add(string(i), 1, 1, 1, 1000)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		kd.delete(string(i))
	}
}

