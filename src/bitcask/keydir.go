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

type Item struct {
	fid int32
	valueSize int32
	valuePos int32
	timeStamp int64
}

// keydir is a index data structure for bitcask
// It is safe to call add, remove, get concurrently.
type KeyDir struct {
	sync.RWMutex
	kv map[string]Item
}


func newKeyDir() *KeyDir {
	return &KeyDir{
		kv: make(map[string]Item),
	}
}


func (kd *KeyDir) add(key string, fid int32, valueSize int32, valuePos int32, timeStamp int64) {
	kd.Lock()
	defer kd.Unlock()

	kd.kv[key] = Item{fid, valueSize, valuePos, timeStamp}

	return
}


func (kd *KeyDir) get(key string) (*Item, bool) {
	kd.RLock()
	defer kd.RUnlock()

	v, b := kd.kv[key]
	return &v, b
}


func (kd *KeyDir) delete(key string) {
	kd.Lock()
	defer kd.Unlock()

	delete(kd.kv, key)
}


func (kd *KeyDir) keys() chan string {
	ch := make(chan string)
	go func() {
		for k, _ := range kd.kv {
			ch <- k
		}
		close(ch)
	}()
	return ch
}

