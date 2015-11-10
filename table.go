package trounoir

import (
	"io/ioutil"
	"os"
)

const (
	DB_PATH = `/Users/kewang/Applications/trounoir/`
)

type Table interface {
	Path() string
	Put(key string, b []byte) error
	Get(key string) ([]byte, error)
}

// create a new table
func CreateTable(Name string) (Table, error) {
	err := os.Mkdir(DB_PATH+Name, 0755)
	if err != nil {
		return nil, err
	}
	return NewTable(Name), nil
}

func NewTable(Name string) Table {
	return &table{Name}
}

type table struct {
	Name string
}

func (t *table) Path() string {
	return DB_PATH + t.Name + "/"
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
