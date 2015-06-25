package bitcask

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"encoding/binary"
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

type Record struct {
	crc    uint32
	tstamp int64
	ksz    int32
	vsz    int32
	key    []byte
	value  []byte
}

func (r *Record) compress() error {
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


func (r *Record) uncompress() error {
	b := bytes.NewReader(r.value)
	zr, err := zlib.NewReader(b)
	if err != nil {
		return fmt.Errorf("uncompress value %s", err.Error())
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, zr)
	if err != nil {
		return fmt.Errorf("Copy failed because of %s", err.Error())
	}
	r.value = buf.Bytes()
	r.vsz = int32(len(buf.Bytes()))
	zr.Close()
	return nil
}

func (r *Record) encode() ([]byte, error) {
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


type File struct {
	io     *os.File
	wbuf   *bufio.Writer
	offset int32
	id     int32
}


func newFile(f *os.File, id int32) *File {
//	Lg.Println("Create file" + f.Name())
	fmt.Println("Create file" + f.Name())
	fi, _ := f.Stat()
	offset := fi.Size()
//	Lg.Printf("offset is %d.", offset)
	fmt.Printf("offset is %d.", offset)
	return &File{
		io:     f,
		wbuf:   bufio.NewWriter(f),
		offset: int32(offset),
		id:     id}
}


func (file *File) write(key string, value []byte, tstamp int64) (int32, error) {
	record := &Record{
		key:    []byte(key),
		value:  value,
		ksz:    int32(len(key)),
		vsz:    int32(len(value)),
		tstamp: tstamp,
	}

	pos, err := file.writeRecord(record)
	if err != nil {
		return -1, err
	}

	return pos, nil
}


func (file *File) read() (*Record, error) {
	record := new(Record)
	header := make([]byte, RECORD_HEADER_SIZE)

	size, err := file.io.Read(header)
	if err != nil {
		return nil, err
	}
	if int32(size) != RECORD_HEADER_SIZE {
		return nil, fmt.Errorf("Read Header: exptectd %d, got %d", RECORD_HEADER_SIZE, size)
	}

	buf := bytes.NewReader(header)
	binary.Read(buf, binary.BigEndian, &record.crc)
	binary.Read(buf, binary.BigEndian, &record.tstamp)
	binary.Read(buf, binary.BigEndian, &record.ksz)
	binary.Read(buf, binary.BigEndian, &record.vsz)

	record.key = make([]byte, record.ksz)
	record.value = make([]byte, record.vsz)
	if _, err := file.io.Read(record.key); err != nil {
		return nil, fmt.Errorf("key: %s", err.Error())
	}
	if _, err := file.io.Read(record.value); err != nil {
		return nil, fmt.Errorf("Read value: %s", err.Error())
	}

	//check crc
	data := append(append(header, record.key...), record.value...)
	crc := crc32.ChecksumIEEE(data[4:])
	if record.crc != crc {
		return nil, fmt.Errorf("CRC check failed %u %u", record.crc, crc)
	}
	return record, err
}


func (file *File) writeRecord(record *Record) (int32, error) {
	data, err := record.encode()
	if err != nil {
		return -1, err
	}

	size, err := file.wbuf.Write(data)
	if err != nil {
		return -1, err
	}
	if size < len(data) {
		err = fmt.Errorf("writeRecord: expected %d got %d\n", len(data), size)
		return -1, err
	}

	valuePos := int32(file.offset + RECORD_HEADER_SIZE + int32(len(record.key)))
	file.offset += int32(size)
	//Lg.Printf("write %s to %s", string(record.key), file.io.Name())
	fmt.Printf("write %s to %s.\n", string(record.key), file.io.Name())

	return valuePos, nil
}


func (file *File) close() error {
	if err := file.wbuf.Flush(); err != nil {
		return err
	}
	return file.io.Close()
}


func (file *File) sync() error {
	if err := file.wbuf.Flush(); err != nil {
		return err
	}
	err := file.io.Sync()
	return err
}


func (file *File) name() string {
	if file.io != nil {
		return file.io.Name()
	}
	return ""
}

