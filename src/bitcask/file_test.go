package bitcask



import (
	"os"
	"testing"
)

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
