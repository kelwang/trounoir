package trounoirDB

import (
	"io/ioutil"
	"os"
)

type Table interface {
	Path() string
	Put(key string, b []byte) error
	Get(key string) ([]byte, error)
}

// CreateTable will create a boltdb file under the folder
func CreateTable(name string, cfg Config) (Table, error) {
	path := cfg.Folder + "/" + name
	err := os.Mkdir(path, 0755)
	if err != nil {
		return nil, err
	}
	return NewTable(name, path), nil
}

// NewTable init a new table object
func NewTable(name, path string) Table {
	return &table{Name: name, path: path}
}

type table struct {
	Name string
	path string
}

func (t *table) Path() string {
	return t.path
}

func (t *table) Put(key string, b []byte) error {
	f, err := os.Create(t.Path() + key)
	defer f.Close()
	if err != nil {
		return err
	}
	f.Write(b)
	return nil
}

func (t *table) Get(key string) ([]byte, error) {
	return ioutil.ReadFile(t.Path() + key)
}
