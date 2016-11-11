package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// Default Request Handler
func defaultHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<h1>Hello %s!</h1>", r.URL.Path[1:])
}

type Args struct {
	A, B int
}

//Represents Arith service for RPC
type Arith int

//Arith service has procedure Multiply which takes numbers A, B as arguments and returns error or stores product in reply
func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Add(args *Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

func main() {
	arith := new(Arith)
	err := rpc.Register(arith)
	if err != nil {
		log.Fatal("Service Error: %s", err)
	}
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", 57439))

	if err != nil {
		log.Fatal("TCP Error: %s", err)
	}
	log.Println("ok")
	http.Serve(l, nil)
}
