package trounoir

import (
	"container/list"
	"github.com/boltdb/bolt"
	"sync"
)

type trounoir struct {
	WriterBuffer *ringBuffer
	ReaderBuffer *ringBuffer
	Bolts        []*bolt.DB
	MemBuffer    []*bucketBuffer
}

func New(bolt_cluster []string, db_name string) {

}

type bucketBuffer struct {
	sync.Mutex
	data      map[string][]byte
	keyQueque *list.List
}

func (bb *bucketBuffer) remove(key string) {
	bb.Lock()
	delete(bb.data, key)
	bb.Unlock()
	go func(k string) {
		var next *list.Element
		for e := bb.keyQueque.Front(); e != nil; e = next {
			next = e.Next()
			if k == e.Value.(string) {
				bb.keyQueque.Remove(e)
			}
		}
	}(key)
}

func (bb *bucketBuffer) add(key string, b []byte) {
	bb.Lock()

	bb.Unlock()
}

type ringBuffer struct {
	inputChannel  <-chan int
	outputChannel chan int
}

func newRingBuffer(inputChannel <-chan int, outputChannel chan int) *ringBuffer {
	return &ringBuffer{inputChannel, outputChannel}
}

func (r *ringBuffer) run() {
	for v := range r.inputChannel {
		select {
		case r.outputChannel <- v:
		default:
			<-r.outputChannel
			r.outputChannel <- v
		}
	}
}
