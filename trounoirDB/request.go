package trounoirDB

import (
	"crypto/md5"
	"fmt"
	"io"
	"time"
)

type Method int

type Request struct {
	IsRemote   bool
	Timestamp  int64
	SecureHash []byte
	Bucket     string
	Key        string
	Value      interface{}
	MRQuery    []byte
}

// generate secure hash with timestamp
// One time password generator for each request
func (r *Request) GenSecure(salt string) {
	r.Timestamp = time.Now().Unix()
	h := md5.New()
	io.WriteString(h, fmt.Sprintf("%d%s", r.Timestamp, salt))
	hh := h.Sum(nil)
	io.WriteString(h, fmt.Sprintf("%d%s%s", r.Timestamp, salt, string(hh)))
	r.SecureHash = h.Sum(nil)
}

//verify if the request is real
func (r *Request) Verify(salt string) bool {
	if len(r.SecureHash) <= 0 {
		return false
	}

	// expire the request after 30 seconds
	if time.Now().Unix()-r.Timestamp > 30 {
		return false
	}

	h := md5.New()
	io.WriteString(h, fmt.Sprintf("%d%s", r.Timestamp, salt))

	hh := h.Sum(nil)
	io.WriteString(h, fmt.Sprintf("%d%s%s", r.Timestamp, salt, string(hh)))
	hh = h.Sum(nil)

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
