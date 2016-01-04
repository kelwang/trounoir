package trounoirDB

import (
	"crypto/md5"
	"fmt"
	"io"
)

type Method int


type Request struct {
	IsRemote   bool
	Timestamp  int
	SecureHash []byte
	Bucket     string
	Key        string
	Value      interface{}
	MRQuery    []byte
}

//verify if the request is real
func (r *Request) Verify(salt string) bool {
	if len(r.SecureHash) <= 0 {
		return false
	}

	h := md5.New()
	io.WriteString(h, fmt.Sprintf("%d%s",r.Timestamp, salt))
	
	hh := h.Sum(nil)
	if len(hh) != len(r.SecureHash) {
		return false
	}
	for k, v := range hh {
		if r.SecureHash[k] != v {
			return false
		}
	}
	return true
}

