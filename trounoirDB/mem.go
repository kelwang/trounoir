package trounoirDB

import (
	"sync"
	"time"
)

type memDataItem struct {
	V         []byte
	Timestamp int64
}

type MemTable struct {
	sync.RWMutex
	Data         map[string]memDataItem
	expireQueque map[int64][]string
	expireChan   chan struct{}
	renewChan    chan string
	// in seconds
	expireTime int64
}

func (mt *MemTable) recursiveExpire() {
	go time.AfterFunc(time.Duration(mt.expireTime*int64(time.Second)), func() {
		mt.expireChan <- struct{}{}
		mt.recursiveExpire()
	})
}

func (mt *MemTable) Run() {
	mt.recursiveExpire()
	for {
		select {
		case key := <-mt.renewChan:
			mt.Lock()
			now := time.Now().Unix()
			var oldExpireTime, newExpireTime int64
			v, ok := mt.Data[key]

			newExpTime := now + mt.expireTime

			mt.Data[key] = memDataItem{Timestamp: newExpireTime, V: v.V}
			if ok {
				oldExpireTime = mt.Data[key].Timestamp
				for i, k := range mt.expireQueque[oldExpireTime] {
					if k == key {
						mt.expireQueque[oldExpireTime] = append(mt.expireQueque[oldExpireTime][:i], mt.expireQueque[oldExpireTime][i+1:]...)
					}
				}
			}
			_, ok = mt.expireQueque[newExpTime]
			if ok {
				mt.expireQueque[newExpTime] = append(mt.expireQueque[newExpTime], key)
			} else {
				mt.expireQueque[newExpTime] = []string{key}
			}
			mt.Unlock()
		case <-mt.expireChan:
			timestamp := time.Now().Unix()
			go func(mt *MemTable) {
				for k := range mt.expireQueque {
					if k < timestamp {
						mt.Lock()
						for _, key := range mt.expireQueque[k] {
							delete(mt.Data, key)
						}
						delete(mt.expireQueque, k)
						mt.Unlock()
					}
				}
			}(mt)
		}

	}
}

func (mt *MemTable) get(key string) ([]byte, bool) {
	mt.RLock()
	defer mt.RUnlock()
	value, ok := mt.Data[key]
	if ok {
		mt.renewChan <- key
	}
	return value.V, ok
}

func (mt *MemTable) add(key string, value []byte) {
	mt.Lock()
	defer mt.Unlock()
	mt.Data[key] = memDataItem{V: value, Timestamp: time.Now().Unix()}

}

func (mt *MemTable) remove(key string) {
	mt.Lock()
	defer mt.Unlock()
	delete(mt.Data, key)
}
