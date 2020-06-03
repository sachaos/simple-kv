package main

import (
	"context"
	"fmt"
)

type StorageKV struct {
	index map[string][]byte
}

func NewStorageKV() *StorageKV {
	return &StorageKV{index: map[string][]byte{}}
}

func (s *StorageKV) Get(ctx context.Context, key string) ([]byte, error) {
	value, ok := s.index[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	return value, nil
}

func (s *StorageKV) Set(ctx context.Context, key string, value []byte) error {
	s.index[key] = value

	return nil
}
