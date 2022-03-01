package owluster

import (
	"github.com/golang/glog"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var (
	node1        = "127.0.0.1:10111"
	kvPort1      = ":10112"
	ClusterPort1 = ":10113"
	node2        = "127.0.0.1:10211"
	kvPort2      = ":10212"
	ClusterPort2 = ":10213"
	node3        = "127.0.0.1:10311"
	kvPort3      = ":10312"
	ClusterPort3 = ":10313"
	node4        = "127.0.0.1:10411"
	kvPort4      = ":10412"
	ClusterPort4 = ":10413"
	cluster      = "127.0.0.1:10111,127.0.0.1:10211,127.0.0.1:10311"
)

func Record(name string) {
	WaitForHealthy()
	ticker := time.NewTicker(15 * time.Second)
	CleanValue(name)
	for {
		select {
		case <-ticker.C:
			i := rand.Int()
			SetValue(name, strconv.Itoa(i))
		}
	}
}

func TestOWL1(t *testing.T) {
	EnableGlogForTesting()

	JoinCluster(node1, cluster)
	StartKVServer(kvPort1)
	StartClusterAPIServer(ClusterPort1)
	go Record("OWL1")

	glog.V(4).Info("STARTED")
	<-make(chan struct{}, 1)
}

func TestOWL2(t *testing.T) {
	EnableGlogForTesting()

	JoinCluster(node2, cluster)
	StartKVServer(kvPort2)
	StartClusterAPIServer(ClusterPort2)
	go Record("OWL2")

	glog.V(4).Info("STARTED")
	<-make(chan struct{}, 1)
}

func TestOWL3(t *testing.T) {
	EnableGlogForTesting()

	JoinCluster(node3, cluster)
	StartKVServer(kvPort3)
	StartClusterAPIServer(ClusterPort3)
	go Record("OWL3")

	glog.V(4).Info("STARTED")
	<-make(chan struct{}, 1)
}

func TestOWL4(t *testing.T) {
	EnableGlogForTesting()

	JoinCluster(node4, cluster)
	StartKVServer(kvPort4)
	StartClusterAPIServer(ClusterPort4)
	go Record("OWL4")

	glog.V(4).Info("STARTED")
	<-make(chan struct{}, 1)
}
