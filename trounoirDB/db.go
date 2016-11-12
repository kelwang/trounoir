package trounoirDB

import (
	"os"
	"sync"

	"github.com/boltdb/bolt"
)

type DB struct {
	path       string
	mode       os.FileMode
	buffer     int
	expireTime int64

	sync.RWMutex
	table map[string]*DBTable
}

type DBTable struct {
	mt     *MemTable
	boltDB *bolt.DB
}

// CreateTable will do the following
// create memTable
// create local boltDB file, return err if can't create
// send rpc to all db
func (db *DB) LoadTable(name string) error {
	db.Lock()
	defer db.Unlock()
	// send rpc request here, in goroutine

	boltDB, err := bolt.Open(db.path+"/"+name, db.mode, nil)
	if err != nil {
		return err
	}

	dbt := &DBTable{
		mt: &MemTable{
			Data:         make(map[string]memDataItem),
			expireQueque: make(map[int64][]string),
			expireChan:   make(chan struct{}, db.buffer),
			renewChan:    make(chan string, db.buffer),
			expireTime:   db.expireTime,
		},
		boltDB: boltDB,
	}
	go dbt.mt.Run()
	db.table[name] = dbt
	return nil
}

func (db *DB) GetTable(name string) *DBTable {
	db.RLock()
	defer db.RUnlock()
	return db.table[name]
}
