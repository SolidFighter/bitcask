package bitcask

/*
   Impletation of keydir
   | --- |      | -----------------------------------------------------------------------------|
   | key | -->  | file id (int32) | value size (int32) | value pos (int32) | time stamp (int64) |
   | --- |      | -----------------------------------------------------------------------------|
*/

import (
	"sync"
)

type item struct {
	fileId    int32
	valueSize int32
	valuePos int32
	timeStamp int64
}

// keydir is a index data structure for bitcask
// It wrap for hashmap(builtin go)
// It is safe to call add, remove, get concurrently.
type keydir struct {
	sync.RWMutex
	data map[string]item
}

func newKeydir() *keydir {
	return &keydir{
		data: make(map[string]item),
	}
}

func (kd *keydir) add(key string, fid int32, valueSize int32, valuePos int32, timeStamp int64) {
	kd.Lock()
	defer kd.Unlock()

	kd.data[key] = item{fid, valueSize, valuePos, timeStamp}

	return
}

func (kd *keydir) get(key string) (*item, bool) {
	kd.RLock()
	defer kd.RUnlock()

	v, b := kd.data[key]
	return &v, b
}

func (kd *keydir) delete(key string) {
	kd.Lock()
	defer kd.Unlock()

	delete(kd.data, key)
}

func (kd *keydir) keys() chan string {
	ch := make(chan string)
	go func() {
		for k, _ := range kd.data {
			ch <- k
		}
		close(ch)
	}()
	return ch
}

