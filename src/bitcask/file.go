package bitcask

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	RECORD_HEADER_SIZE int32 = 20
)

/*
    Each record is stored in the following format:
   	|-----------------------------------------------------------------------------------------|
	|crc (uint32) | tstamp (int64) | ksz(int32) | vsz (int32) | key ([]byte) | value ([]byte) |
	|-----------------------------------------------------------------------------------------|
*/

type record struct {
	crc    uint32
	tstamp int64
	ksz    int32
	vsz    int32
	key    []byte
	value  []byte
}

func (r *record) compress() error {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	if _, err := w.Write(r.value); err != nil {
		return fmt.Errorf("compress value : %s", err.Error())
	}
	w.Close()
	r.value = []byte(b.Bytes())
	r.vsz = int32(len(r.value))
	return nil
}

func (r *record) uncompress() error {
	b := bytes.NewReader(r.value)
	zr, err := zlib.NewReader(b)
	if err != nil {
		return fmt.Errorf("uncompress value %s", err.Error())
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, zr)
	r.value = buf.Bytes()
	zr.Close()
	return nil
}

func (r *record) encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, r.tstamp)
	binary.Write(buf, binary.BigEndian, r.ksz)
	binary.Write(buf, binary.BigEndian, r.vsz)
	buf.Write(r.key)
	buf.Write(r.value)
	crc := crc32.ChecksumIEEE(buf.Bytes())

	buf2 := new(bytes.Buffer)
	binary.Write(buf2, binary.BigEndian, crc)
	buf2.Write(buf.Bytes())

	return buf2.Bytes(), nil
}


type file struct {
	io     *os.File
	wbuf   *bufio.Writer
	offset int32
	id     int32
}


func newFile(f *os.File, id int32) *file {
	Lg.Println("Create file" + f.Name())
	fi, _ := f.Stat()
	offset := fi.Size()
	return &file{
		io:     f,
		wbuf:   bufio.NewWriter(f),
		offset: int32(offset),
		id:     id}
}


func (f *file) write(key string, value []byte, tstamp int64) (int32, error) {
	r := &record{
		key:    []byte(key),
		value:  value,
		ksz:    int32(len(key)),
		vsz:    int32(len(value)),
		tstamp: tstamp,
	}

	var pos int32
	var err error
	if pos, err = f.writeRecord(r); err != nil {
		return -1, err
	}

	return pos, nil
}


func (f *file) writeRecord(r *record) (int32, error) {
	data, err := r.encode()
	if err != nil {
		return -1, err
	}
	sz, err := f.wbuf.Write(data)
	if err != nil {
		return -1, err
	}
	if sz < len(data) {
		err = fmt.Errorf("writeRecord: expected %d got %d\n", len(data), sz)
		return -1, err
	}

	valuePos := int32(f.offset + RECORD_HEADER_SIZE + int32(len(r.key)))
	f.offset += int32(sz)
	Lg.Printf("write %s to %s", string(r.key), f.io.Name())
	return valuePos, nil
}

func (f *file) sync() error {
	if err := f.wbuf.Flush(); err != nil {
		return err
	}
	e := f.io.Sync()
	return e
}

func (f *file) read() (*record, error) {
	r := new(record)
	headerData := make([]byte, RECORD_HEADER_SIZE)

	var sz int
	var err error
	if sz, err = f.io.Read(headerData); err != nil {
		return nil, err
	}
	if int32(sz) != RECORD_HEADER_SIZE {
		return nil, fmt.Errorf("Read Header: exptectd %d, got %d", RECORD_HEADER_SIZE, sz)
	}
	buf := bytes.NewReader(headerData)
	binary.Read(buf, binary.BigEndian, &r.crc)
	binary.Read(buf, binary.BigEndian, &r.tstamp)
	binary.Read(buf, binary.BigEndian, &r.ksz)
	binary.Read(buf, binary.BigEndian, &r.vsz)
	r.key = make([]byte, r.ksz)
	r.value = make([]byte, r.vsz)
	if _, err := f.io.Read(r.key); err != nil {
		return nil, fmt.Errorf("key: %s", err.Error())
	}
	if _, err := f.io.Read(r.value); err != nil {
		return nil, fmt.Errorf("Read value: %s", err.Error())
	}

	//check crc
	data := append(append(headerData, r.key...), r.value...)
	crc := crc32.ChecksumIEEE(data[4:])
	if r.crc != crc {
		return nil, fmt.Errorf("CRC check failed %u %u", r.crc, crc)
	}
	return r, err
}

func (f *file) name() string {
	if f.io != nil {
		return f.io.Name()
	}
	return ""
}

func (f *file) close() error {
	if err := f.wbuf.Flush(); err != nil {
		return err
	}
	return f.io.Close()
}

