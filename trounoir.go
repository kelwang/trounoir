package trounoir

import (
	"container/list"
	"github.com/boltdb/bolt"
	"strings"
	"sync"
)

const (
	MAX_MEM_BUFFER_KEY = 2000
)

// collection of db
type trounoir struct {
	InputChannel    chan *item
	OutputChannel   chan *item
	Bolts           []*bolt.DB
	MemBuffer       map[string]*bucketBuffer
	NumMemBufferKey int
	keyQueque       *list.List
}

func New(bolt_cluster []string, db_name string) *trounoir {
	tr := &trounoir{}
	tr.InputChannel = make(chan *item)
	tr.OutputChannel = make(chan *item, len(bolt_cluster))
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

// add a key to bucket
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

type item struct {
	bucket string
	key    string
	b      []byte
}

func (tr *trounoir) process() {
	for i := range tr.Bolts {
		out := <-tr.OutputChannel
		tr.Bolts[i].Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(out.bucket))
			return b.Put([]byte(out.key), out.b)
		})
	}
}
