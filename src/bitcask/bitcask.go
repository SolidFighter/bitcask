package bitcask

import (
	"os"
	"sync"
	"log"
	"errors"
	"fmt"
	"time"
	"path"
	"sort"
	"io"
)

const (
	LOGFILE         string      = "bitcask.log"
	defaultFilePerm os.FileMode = 0666
	defaultDirPerm  os.FileMode = 0766
	DATA_FILE       string      = "%09d.data"
)

type Stats struct {
	total int64
	dead      int64
	isMerging bool
}


type Options struct {
	MaxFileSize  int32
//	MergeWindow  [2]int // startTime-EndTime
//	MergeTrigger float32
	Path         string
}


type Bitcask struct {
	Stats
	Options
	sync.Mutex
	curr *file
	kd *keydir
}


var ErrKeyNotFound = errors.New("Key not found")
var Lg *log.Logger


//Set log file
func init() {
	os.Remove(LOGFILE)
	logfile, _ := os.OpenFile(LOGFILE, os.O_RDWR|os.O_CREATE, defaultFilePerm)
	Lg = log.New(logfile, "\n", log.Ldate|log.Ltime|log.Llongfile)
}


func NewBitcask(o Options) (*Bitcask, error) {
	err := os.MkdirAll(o.Path, defaultDirPerm)
	if err != nil {
		return nil, fmt.Errorf("Make dir %s %s", o.Path, err.Error())
	}

	b := new(Bitcask)
	b.kd = newKeydir()
	b.Options = o

	err = b.scan()
//	go b.merge()
	return b, err
}


func (b *Bitcask) Close() error {
	if err := b.Sync(); err != nil {
		return err
	}
	return b.curr.close()
}


func (b *Bitcask) Sync() error {
	return b.curr.sync()
}


func (b *Bitcask) Set(key string, value []byte) error {
	b.Lock()
	defer b.Unlock()
	err := b.set(key, value, time.Now().Unix())
	return err
}


func (b *Bitcask) Get(key string) ([]byte, error) {
	item, ok := b.kd.get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}
	value, err := b.getValue(item)
	return value, err
}


func (b *Bitcask) Del(key string) error {
	b.Lock()
	defer b.Unlock()

	value := []byte{0}
	err := b.set(key, value, time.Now().Unix())
	b.kd.delete(key)

	return err
}

func (b *Bitcask) Keys() chan string {
	return b.kd.keys()
}


func (b *Bitcask) Has(key string) bool {
	_, ok := b.kd.get(key)
	return ok
}


func (b *Bitcask) set(key string, value []byte, tstamp int64) error {
	if len(key) == 0 {
		return fmt.Errorf("Key can not be None")
	}

	if RECORD_HEADER_SIZE+int32(len(key)+len(value))+b.curr.offset > b.MaxFileSize {
		if err := b.curr.close(); err != nil {
			return fmt.Errorf("Close %s failed: %s", b.curr.io.Name(), err.Error())
		}
		nextFid := b.curr.id + 1
		nextPath := path.Join(b.Path, fmt.Sprintf(DATA_FILE, nextFid))
		nextFp, err := os.OpenFile(nextPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, defaultDirPerm)
		if err != nil {
			return fmt.Errorf("Create %s failed :%s", nextFp.Name(), err.Error())
		}
		b.curr = newFile(nextFp, nextFid)
	}
	vpos, err := b.curr.write(key, value, tstamp)
	if err != nil {
		return err
	}

	if b.Has(key) {
		b.dead++
	}
	b.kd.add(key, b.curr.id, int32(len(value)), vpos, tstamp)
	b.total++

	return nil
}


func (b *Bitcask) getValue(it *item) ([]byte, error) {
	fp, err := os.Open(path.Join(b.Path, fmt.Sprintf(DATA_FILE, it.fileId)))
	if err != nil {
		return nil, fmt.Errorf("getValue %s", err.Error())
	}
	defer fp.Close()
	value := make([]byte, it.valueSize)
	realSize, err := fp.ReadAt(value, int64(it.valuePos))
	if int32(realSize) != it.valueSize {
		return nil, fmt.Errorf("Expected %d bytes got %d", it.valueSize, realSize)
	}
	return value, nil
}


func getFileNames(dirPath string) ([]string, error) {
	var (
		dir *os.File
		err error
	)
	if dir, err = os.Open(dirPath); err != nil {
		return nil, err
	}
	defer dir.Close()

	fns, _ := dir.Readdirnames(-1)
	sort.Strings(fns)
	return fns, nil
}


func (b *Bitcask) fillKeydir(fn string) error {
	f, err := os.Open(path.Join(b.Path, fn))
	if err != nil {
		return fmt.Errorf("FillKeydir : %s", err.Error())
	}
	defer f.Close()

	var fid int32
	fmt.Sscanf(fn, DATA_FILE, &fid)
	fl := newFile(f, fid)
	var (
		toreturn error
		offset   int32 = 0
	)
	for {
		r, err := fl.read()
		if err != nil {
			if err != io.EOF {
				toreturn = err
			}
			break
		}

		offset += RECORD_HEADER_SIZE + r.ksz + r.vsz
		key := string(r.key)
		if b.isMerging {
			b.Lock()
			if it, ok := b.kd.get(key); ok {
				if r.tstamp == it.timeStamp {
					b.total--
					err = b.set(key, r.value, r.tstamp)
					if err != nil {
						return fmt.Errorf("Failed to set", err.Error())
					}
				}
			} else {
				b.dead--
			}
			b.Unlock()
		} else {
			// valid item
			if r.vsz != 1 || r.value[0] != 0 {
				b.total++
				b.kd.add(key, fid, r.vsz, offset-r.vsz, r.tstamp)
				if b.Has(key) {
					b.dead++
				}
			} else { // invalid item(delete)
				b.dead++
			}
		}
	}
	return toreturn
}


func (b *Bitcask) scan() error {
	fns, err := getFileNames(b.Path)
	if err != nil {
		return err
	}
	for _, f := range fns {
		err := b.fillKeydir(f)
		if err != nil {
			return fmt.Errorf("scan fillkeydir : %s", err.Error())
		}
	}

	// choose active file
	var activeFilePath string
	var fid int32
	if len(fns) == 0 {
		activeFilePath = path.Join(b.Path, fmt.Sprintf(DATA_FILE, 0))
		fid = 0
	} else {
		activeFilePath = path.Join(b.Path, fns[len(fns)-1])
		fid = int32(len(fns)) - 1
	}
	Lg.Println("Open activefile " + activeFilePath)
	var activefile *os.File
	activefile, err = os.OpenFile(activeFilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	b.curr = newFile(activefile, fid)

	return err
}

