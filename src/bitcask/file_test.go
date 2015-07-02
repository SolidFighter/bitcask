package bitcask

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

const MB = 1024 * 1024

func TestNewFile(t *testing.T) {
	activefile, _ := os.OpenFile("1.data", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	file := newFile(activefile, 1)
	fmt.Printf("offset = %d, id = %d.\n", file.offset, file.id)
	os.Remove("1.data")
}

func TestFileWriteRead(t *testing.T) {
	activefile, _ := os.OpenFile("1.data", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	file := newFile(activefile, 1)
	fmt.Printf("offset = %d, id = %d.\n", file.offset, file.id)
	file.write("key", []byte("value"), time.Now().Unix())
	file.write("key1", []byte("value1"), time.Now().Unix())
	file.close()

	activefile, _ = os.OpenFile("1.data", os.O_RDONLY, 0766)
	file = newFile(activefile, 1)
	record, err := file.read()
	if err != nil || !reflect.DeepEqual(string(record.key), "key") || !reflect.DeepEqual(string(record.value), "value") {
		t.Errorf("read failed, record.key = %s, record.value = %s.", record.key, record.value)
	}

	record, err = file.read()
	if err != nil || !reflect.DeepEqual(string(record.key), "key1") || !reflect.DeepEqual(string(record.value), "value1") {
		t.Errorf("read failed, record.key = %s, record.value = %s.", record.key, record.value)
	}
	os.Remove("1.data")
}

func TestName(t *testing.T) {
	activefile, _ := os.OpenFile("1.data", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	file := newFile(activefile, 1)

	if file.name() != "1.data" {
		t.Errorf("Expected %s, Got %s", "1.data", file.name())
	}
	os.Remove("1.data")
}

//Inner func
func genValue(size int) []byte {
	v := make([]byte, size)
	for i := 0; i < size; i++ {
		v[i] = uint8((rand.Int() % 26) + 97) // a-z
	}
	return v
}
