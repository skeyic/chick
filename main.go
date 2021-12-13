package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/skeyic/chick/owluster"
)

/*
chick.exe --port :10111 --cluster 127.0.0.1:10222,127.0.0.1:10333 --id 1
chick.exe --port :10222 --cluster 127.0.0.1:10111,127.0.0.1:10333 --id 2
chick.exe --port :10333 --cluster 127.0.0.1:10111,127.0.0.1:10222 --id 3
*/

func main() {
	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9091", "comma sep")
	id := flag.Int("id", 1, "node ID")

	flag.Set("logtostderr", "true")
	flag.Set("v", "4")

	flag.Parse()
	processC := make(chan string)
	owluster.NewRaft(*id, *port, *cluster, processC)

	glog.V(4).Info("STARTED")

	<-make(chan struct{}, 1)
}
