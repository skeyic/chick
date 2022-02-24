package owluster

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"testing"
)

func TestRPC(t *testing.T) {
	EnableGlogForTesting()

	rpc.Register(new(Owl))
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":9001")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	select {}
}

func TestRPCClient2(t *testing.T) {
	EnableGlogForTesting()

	client, err := rpc.DialHTTP("tcp", ":9001")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	args := &VoteArgs{999, 1, "127.0.0.1:9001", false}
	var reply = new(VoteReply)
	err = client.Call("Owl.RequestVote", args, &reply)
	if err != nil {
		log.Fatal("RequestVote error:", err)
	}
	fmt.Printf("REPLY: %v", reply)
}
