package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/skeyic/chick/owluster"
)

/*
chick.exe --address 127.0.0.1:10111 --cluster 127.0.0.1:10111,127.0.0.1:10222,127.0.0.1:10333 --http_port :10112
chick.exe --address 127.0.0.1:10222 --cluster 127.0.0.1:10111,127.0.0.1:10222,127.0.0.1:10333 --http_port :10223
chick.exe --address 127.0.0.1:10333 --cluster 127.0.0.1:10111,127.0.0.1:10222,127.0.0.1:10333 --http_port :10334
chick.exe --address 127.0.0.1:10444 --cluster 127.0.0.1:10111,127.0.0.1:10222,127.0.0.1:10333 --http_port :10445
*/

func main() {
	address := flag.String("address", "127.0.0.1:9091", "address in cluster")
	cluster := flag.String("cluster", "127.0.0.1:9091,127.0.0.1:9093,127.0.0.1:9095", "comma sep")
	httpport := flag.String("http_port", ":9092", "http listen port")

	flag.Set("logtostderr", "true")
	flag.Set("v", "4")

	flag.Parse()
	beak := owluster.NewBeak()
	data := owluster.NewGlobalData()
	owluster.NewOwl(*address, *cluster, beak, owluster.NewGlobalMap(data))
	owluster.ServeHttpKVAPI(*httpport, owluster.NewServer(beak))

	glog.V(4).Info("STARTED")
	<-make(chan struct{}, 1)
}
