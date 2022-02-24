package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/skeyic/chick/owluster"
)

/*
chick.exe --address 127.0.0.1:10111 --cluster 127.0.0.1:10111,127.0.0.1:10211,127.0.0.1:10311 --kv_port :10112 --cluster_port :10113
chick.exe --address 127.0.0.1:10212 --cluster 127.0.0.1:10111,127.0.0.1:10211,127.0.0.1:10311 --kv_port :10212 --cluster_port :10213
chick.exe --address 127.0.0.1:10313 --cluster 127.0.0.1:10111,127.0.0.1:10211,127.0.0.1:10311 --kv_port :10312 --cluster_port :10313
chick.exe --address 127.0.0.1:10414 --cluster 127.0.0.1:10111,127.0.0.1:10211,127.0.0.1:10311 --kv_port :10412 --cluster_port :10413
*/

func main() {
	address := flag.String("address", "127.0.0.1:10111", "address in cluster")
	cluster := flag.String("cluster", "127.0.0.1:10111,127.0.0.1:10211,127.0.0.1:10311", "comma sep")
	kvport := flag.String("kv_port", ":10112", "kv server listen port")
	clusterport := flag.String("cluster_port", ":10113", "cluster server listen port")

	flag.Set("logtostderr", "true")
	flag.Set("v", "4")

	flag.Parse()
	owluster.JoinCluster(*address, *cluster)
	owluster.StartKVServer(*kvport)
	owluster.StartClusterAPIServer(*clusterport)

	glog.V(4).Info("STARTED")
	<-make(chan struct{}, 1)
}
