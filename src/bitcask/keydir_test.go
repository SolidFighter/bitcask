package bitcask

import (
	"testing"
	"fmt"
)

func Test(t *testing.T) {
	kd := newKeydir()
	kd.add("fvck", 1, 1, 1, 1000)
	kd.add("fvck1", 1, 1, 1, 1001)
	it, _ := kd.get("fvck")
	if it.fileId != 1 && it.timeStamp != 1000 && it.valuePos != 1 && it.valueSize != 1{
		t.Errorf("get(fvck) failed.")
	}
	it, _ = kd.get("fvck1")
	if it.fileId != 1 && it.timeStamp != 1001 && it.valuePos != 1 && it.valueSize != 1{
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


