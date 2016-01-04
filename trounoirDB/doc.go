package trounoirDB

import (
	"io"
	"sync"
)

type Doc interface {
}

type doc struct {
	Key    string
	Reader io.Reader
	sync.Mutex
}

// insert or update
func (d *doc) Put(key string, r io.Reader) error {
	return nil
}

// delete
func (d *doc) Delete(key string) error {
	return nil
}
