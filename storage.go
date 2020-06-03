package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

type Segment struct {
	index map[string]int64
	head int64
	reader io.ReadSeeker
	writer io.Writer
}

func NewSegment(reader io.ReadSeeker, writer io.Writer, head int64) *Segment {
	return &Segment{index: map[string]int64{}, reader: reader, writer: writer, head: head}
}

func (s *Segment) Get(ctx context.Context, key string) ([]byte, error) {
	offset, ok := s.index[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	keyLengthByte := make([]byte, 1)
	_, err := s.reader.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	_, err = s.reader.Read(keyLengthByte)
	if err != nil {
		return nil, err
	}

	keyLength := int(keyLengthByte[0])
	keyByte := make([]byte, keyLength)

	_, err = s.reader.Read(keyByte)
	if err != nil {
		return nil, err
	}

	if key != string(keyByte) {
		return nil, fmt.Errorf("key mismatch")
	}

	valueLengthByte := make([]byte, 2)
	_, err = s.reader.Read(valueLengthByte)
	if err != nil {
		return nil, err
	}

	valueLength := binary.BigEndian.Uint16(valueLengthByte)
	valueByte := make([]byte, valueLength)
	_, err = s.reader.Read(valueByte)
	if err != nil {
		return nil, err
	}

	return valueByte, nil
}

func (s *Segment) Set(ctx context.Context, key string, value []byte) error {
	log := make([]byte, 1+len(key)+2+len(value))
	log[0] = byte(len(key))
	for i := 0; i < len(key); i++ {
		log[1 + i] = key[i]
	}

	binary.BigEndian.PutUint16(log[1+len(key):], uint16(len(value)))

	for i := 0; i < len(value); i++ {
		log[1 + len(key) + 2 + i] = value[i]
	}

	length := int64(len(log))

	_, err := s.writer.Write(log)
	if err != nil {
		return err
	}

	s.index[key] = s.head
	s.head += length

	logrus.Debugf("index: %+v", s.index)

	return nil
}

type StorageKV struct {
	segments []*Segment
}

func NewStorageKV() *StorageKV {
	file, err := os.OpenFile("./log", os.O_APPEND|os.O_RDWR|os.O_CREATE, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	segments := []*Segment{
		NewSegment(file, file, stat.Size()),
	}
	return &StorageKV{segments: segments}
}

func (s *StorageKV) Get(ctx context.Context, key string) ([]byte, error) {
	return s.segments[0].Get(ctx, key)
}

func (s *StorageKV) Set(ctx context.Context, key string, value []byte) error {
	return s.segments[0].Set(ctx, key, value)
}
