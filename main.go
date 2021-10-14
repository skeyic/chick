package main

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/skeyic/chick/app/router"
	"github.com/skeyic/chick/cluster"
	"github.com/skeyic/chick/config"
)

func main() {
	var (
		myID = cluster.NodeID
	)

	flag.Parse()

	r := router.InitRouter()
	glog.V(4).Infof("CHICK %s starts...", myID)
	glog.Fatal(r.Run(fmt.Sprintf(":%d", config.Config.Port)))
}
