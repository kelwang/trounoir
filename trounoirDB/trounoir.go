package trounoirDB

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
)

const (
	MAX_MEM_BUFFER_KEY = 2000
)

var (
	ERR_REQUEST_NOT_VERIFIED = errors.New("The remote request is not verified")
	ERR_MISSING_BUCKET       = errors.New("Missing bucket")
	ERR_MISSING_KEY          = errors.New("Missing Key")
	ERR_MISSING_VALUE        = errors.New("Missing Value")
)

type Trounoir struct {
	Config
	LocalConfig
	Bolt            *bolt.DB
	MemBuffer       map[string][]byte
	NumMemBufferKey int
	keyQueque       *list.List
}

func (t *Trounoir) Fetch(r *Request, result *[]byte) error {

	if r.IsRemote {
		if !r.Verify(t.Config.Salt) {
			return ERR_REQUEST_NOT_VERIFIED
		}
	}

	if r.Bucket == "" {
		return ERR_MISSING_BUCKET
	}

	if r.Key == "" {
		return ERR_MISSING_KEY
	}

	//search memcache
	if rlt, ok := t.MemBuffer[r.Bucket+" "+r.Key]; ok {
		*result = rlt
		return nil
	}

	first_result_chan := make(chan []byte)

	go func() {
		//search bolt
		t.Bolt.View(func(tx *bolt.Tx) error {
			bu := tx.Bucket([]byte(r.Bucket))
			first_result_chan <- bu.Get([]byte(r.Key))
			return nil
		})
	}()

	if !r.IsRemote {
		//search remote
		err_chan := make(chan error)
		remote_result_chan := make(chan []byte)

		for k := range t.Items {
			if !t.Items[k].IsLocal {
				go func(k int) {
					var res []byte
					err := clientCall("Trounoir.Get", r, t.Items[k].Host, t.Config.Port, &res)
					if err != nil {
						err_chan <- err
					}
					remote_result_chan <- res
				}(k)
			}
		}
		for {
			select {
			case err := <-err_chan:
				return err
			case res := <-remote_result_chan:
				first_result_chan <- res
			}
		}
	}

	*result = <-first_result_chan
	return nil
}

func clientCall(method string, r *Request, host string, port int, result *[]byte) error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	return client.Call(method, r, result)
}

// trounoir struct
//
type trounoir struct {
	InputChannel    chan *item
	BroadcastQueue  *list.List
	Bolt            *bolt.DB
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

func (tr *trounoir) writeToBolt(bucket string, key string, b []byte) {
	tr.Bolt.Begin(true)
	tr.Bolt.Update(func(tx *bolt.Tx) error {
		bu := tx.Bucket([]byte(bucket))
		return bu.Put([]byte(key), b)
	})
	defer tr.Bolt.Close()

}

func (tr *trounoir) readFromBolt(bucket string, key string, first_react_chan chan<- []byte) error {
	tr.Bolt.View(func(tx *bolt.Tx) error {
		bu := tx.Bucket([]byte(bucket))
		first_react_chan <- bu.Get([]byte(key))
		return nil
	})

	return nil
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

func (tr *trounoir) server(port int, err_chan chan<- error) {
	rpc.HandleHTTP()
	//rpc.Register(trouno)
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if e != nil {
		err_chan <- e
	}
	go http.Serve(l, nil)
}

func (tr *trounoir) client(address string, port int) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", address, port))
}
