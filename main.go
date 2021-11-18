package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/skeyic/chick/cluster"
)

func main() {
	var (
		myID = cluster.NodeID
	)

	nodes := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	//kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")

	flag.Set("logtostderr", "true")
	flag.Set("v", "10")

	flag.Parse()

	//cluster.StartServe(*nodes, *id, *kvport, *join)
	cluster.StartOwlServe(*nodes, *id, *join)

	//r := router.InitRouter()
	glog.V(4).Infof("CHICK %s starts...", myID)
	//glog.Fatal(r.Run(fmt.Sprintf(":%d", config.Config.Port)))

	<-make(chan struct{}, 1)
}
