package trounoir

import (
	"container/list"
	"fmt"
	"github.com/boltdb/bolt"
	"net/rpc"
	"strings"
	"sync"
)

const (
	MAX_MEM_BUFFER_KEY = 2000
)

// trounoir struct
//
type trounoir struct {
	InputChannel    chan *item
	BroadcastQueue  *list.List
	Bolts           []*bolt.DB
	MemBuffer       map[string]*bucketBuffer
	NumMemBufferKey int
	keyQueque       *list.List
}

func New(bolt_cluster []string, db_name string) *trounoir {
	tr := &trounoir{}
	tr.InputChannel = make(chan *item)
	return tr
}

type bucketBuffer struct {
	sync.Mutex
	data map[string][]byte
}

func (bb *bucketBuffer) remove(key string) {
	bb.Lock()
	delete(bb.data, key)
	bb.Unlock()
}

// add a key, value to bucket
// forward the item to tr.InputChannel
func (tr *trounoir) add(bucket string, key string, b []byte) {
	bb := tr.MemBuffer[bucket]
	bb.Lock()
	tr.InputChannel <- &item{bucket, key, b}
	bb.data[key] = b
	tr.keyQueque.PushFront(bucket + " " + key)
	if tr.NumMemBufferKey > MAX_MEM_BUFFER_KEY {
		last_bucket_key := tr.keyQueque.Back()
		i := strings.Index(last_bucket_key.Value.(string), " ")
		b2 := last_bucket_key.Value.(string)[:i]
		k2 := last_bucket_key.Value.(string)[i+1:]
		tr.keyQueque.Remove(last_bucket_key)
		delete(tr.MemBuffer[b2].data, k2)
	} else {
		tr.NumMemBufferKey++
	}
	bb.Unlock()
}

func (tr *trounoir) broadcast() {
	for {
		select {
		case item := <-tr.InputChannel:
			tr.BroadcastQueue.PushFront(item)
		}
	}
}

type item struct {
	bucket string
	key    string
	b      []byte
}

func (tr *trounoir) process() {

}

func (tr *trounoir) server() {
	rpc.HandleHTTP()
}

func (tr *trounoir) client(address string, port int) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", address, port))
}
