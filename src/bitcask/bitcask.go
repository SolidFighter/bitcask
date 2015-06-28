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
	totalKeys int64
	deadKeys  int64
	isMerging bool
}


type Options struct {
	MaxFileSize  int32
	//MergeWindow  [2]int // startTime-EndTime
	MergeTrigger float32
	Path         string
}


type Bitcask struct {
	Stats
	Options
	sync.Mutex
	file *File
	kd *KeyDir
}


//var ErrKeyNotFound = errors.New("Key not found")
var Logger *log.Logger


//Set log file
func init() {
	os.Remove(LOGFILE)
	logfile, _ := os.OpenFile(LOGFILE, os.O_RDWR|os.O_CREATE, defaultFilePerm)
	Logger = log.New(logfile, "\n", log.Ldate|log.Ltime|log.Llongfile)
}


func NewBitcask(options Options) (*Bitcask, error) {
	err := os.MkdirAll(options.Path, defaultDirPerm)
	if err != nil {
		return nil, fmt.Errorf("Make dir %s %s", options.Path, err.Error())
	}

	b := new(Bitcask)
	b.kd = newKeyDir()
	b.Options = options

	err = b.scan()
//	go b.merge()
	return b, err
}


func (b *Bitcask) scan() error {
	fns, err := getFileNames(b.Path)
	if err != nil {
		return err
	}
	for _, f := range fns {
		err := b.fillKeyDir(f)
		if err != nil {
			return fmt.Errorf("scan fillkeydir : %s", err.Error())
		}
	}

	// choose active file
	var activeFileName string
	var fid int32
	if len(fns) == 0 {
		activeFileName = path.Join(b.Path, fmt.Sprintf(DATA_FILE, 0))
		fid = 0
	} else {
		activeFileName = path.Join(b.Path, fns[len(fns)-1])
		fid = int32(len(fns)) - 1
	}
	Logger.Println("Open activefile " + activeFileName)
	var activefile *os.File
	activefile, err = os.OpenFile(activeFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	b.file = newFile(activefile, fid)

	return err
}


func getFileNames(dirPath string) ([]string, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	fns, _ := dir.Readdirnames(-1)
	sort.Strings(fns)
	return fns, nil
}


func (b *Bitcask) fillKeyDir(fn string) error {
	f, err := os.Open(path.Join(b.Path, fn))
	if err != nil {
		return fmt.Errorf("FillKeydir : %s", err.Error())
	}
	defer f.Close()

	var fid int32
	fmt.Sscanf(fn, DATA_FILE, &fid)
	file := newFile(f, fid)

	var offset int32 = 0
	for {
		record, err := file.read()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		offset += RECORD_HEADER_SIZE + record.ksz + record.vsz

		key := string(record.key)
		if b.isMerging {
			/*
			b.Lock()
			if item, ok := b.kd.get(key); ok {
				if record.tstamp == item.timeStamp {
					//b.total--
					err = b.set(key, record.value, record.tstamp)
					if err != nil {
						return fmt.Errorf("Failed to set", err.Error())
					}
				}
			} else {
				b.dead--
			}
			b.Unlock()
			*/
		} else {
			// deleted key
			if record.vsz == 1 && record.value[0] == 0 {
				b.kd.delete(string(record.key))
				b.deadKeys++
			} else {
				b.totalKeys++
				b.kd.add(key, fid, record.vsz, offset - record.vsz, record.tstamp)
				if b.Has(key) {
					b.deadKeys++
				}
			}
		}
	}
	return nil
}


func (b *Bitcask) Has(key string) bool {
	_, ok := b.kd.get(key)
	return ok
}


func (b *Bitcask) Close() error {
	if err := b.Sync(); err != nil {
		return err
	}
	return b.file.close()
}


func (b *Bitcask) Sync() error {
	return b.file.sync()
}


func (b *Bitcask) Set(key string, value []byte) error {
	b.Lock()
	defer b.Unlock()
	err := b.set(key, value, time.Now().Unix())
	return err
}


func (b *Bitcask) set(key string, value []byte, tstamp int64) error {
	if len(key) == 0 {
		return fmt.Errorf("Key can not be None")
	}

	if RECORD_HEADER_SIZE+int32(len(key)+len(value))+b.file.offset > b.MaxFileSize {
		if err := b.file.close(); err != nil {
			return fmt.Errorf("Close %s failed: %s", b.file.io.Name(), err.Error())
		}
		nextFid := b.file.id + 1
		nextFileName := path.Join(b.Path, fmt.Sprintf(DATA_FILE, nextFid))
		nextFp, err := os.OpenFile(nextFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, defaultDirPerm)
		if err != nil {
			return fmt.Errorf("Create %s failed :%s", nextFp.Name(), err.Error())
		}
		b.file= newFile(nextFp, nextFid)
	}
	vpos, err := b.file.write(key, value, tstamp)
	if err != nil {
		return err
	}

	if b.Has(key) {
		b.deadKeys++
	}
	b.kd.add(key, b.file.id, int32(len(value)), vpos, tstamp)
	b.totalKeys++

	return nil
}


func (b *Bitcask) Get(key string) ([]byte, error) {
	item, ok := b.kd.get(key)
	if !ok {
		return nil, errors.New("Key not found")
	}
	value, err := b.getValue(item)
	return value, err
}


func (b *Bitcask) getValue(item *Item) ([]byte, error) {
	fp, err := os.Open(path.Join(b.Path, fmt.Sprintf(DATA_FILE, item.fid)))
	if err != nil {
		return nil, fmt.Errorf("getValue %s", err.Error())
	}
	defer fp.Close()
	value := make([]byte, item.valueSize)
	realSize, err := fp.ReadAt(value, int64(item.valuePos))
	if int32(realSize) != item.valueSize {
		return nil, fmt.Errorf("Expected %d bytes but got %d", item.valueSize, realSize)
	}

	return value, nil
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



/*
func (b *Bitcask) merge() {
	for {
		if float32(b.dead)/float32(b.total) > b.MergeTrigger {
			h := time.Now().Hour()
			if h <= b.MergeWindow[1] &&
			h >= b.MergeWindow[0] {
				b.doMerge()
			}
		}
		time.Sleep(10 * time.Minute)
	}
}


func (b *Bitcask) doMergeLater() {
	time.Sleep(time.Minute)
	b.doMerge()
}


func (b *Bitcask) doMerge() {
	b.Lock()
	fns, err := getFileNames(b.Path)
	b.Unlock()
	if err != nil {
		b.doMergeLater()
		return
	}
	fns = fns[:len(fns)-1]

	b.isMerging = true
	for _, fn := range fns {
		if err := b.fillKeydir(fn); err != nil {
			b.isMerging = false
			if err != nil {
				b.doMergeLater()
				return
			}
		}
		time.Sleep(10 * time.Second)
	}
	time.Sleep(10 * time.Minute)

	for _, fn := range fns {
		os.Remove(path.Join(b.Path, fn))
	}
	return
}
*/
