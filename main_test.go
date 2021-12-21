package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/skeyic/chick/owluster"
	"testing"
)

func TestNode3(t *testing.T) {
	flag.Set("logtostderr", "true")
	flag.Set("v", "5")

	flag.Parse()
	beak := owluster.NewBeak()
	owluster.NewOwl("127.0.0.1:10333", "127.0.0.1:10111,127.0.0.1:10222,127.0.0.1:10333", beak, owluster.NewGlobalMap())
	owluster.ServeHttpKVAPI(":10334", owluster.NewServer(beak))

	glog.V(4).Info("STARTED")
	<-make(chan struct{}, 1)
}
