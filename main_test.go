package main

import (
	"flag"
	"github.com/golang/glog"
	"testing"
)

func TestNode3(t *testing.T) {
	flag.Set("logtostderr", "true")
	flag.Set("v", "5")

	flag.Parse()

	glog.V(4).Info("STARTED")
	<-make(chan struct{}, 1)
}
