package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/skeyic/chick/owluster"
)

func main() {
	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9091", "comma sep")
	id := flag.Int("id", 1, "node ID")

	flag.Set("logtostderr", "true")
	flag.Set("v", "10")

	flag.Parse()
	owluster.NewRaft(*id, *port, *cluster)

	glog.V(4).Info("STARTED")

	<-make(chan struct{}, 1)
}
