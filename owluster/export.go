package owluster

import (
	"encoding/json"
	"github.com/golang/glog"
	"log"
	"net/http"
	"time"
)

var (
	TheBeak    = NewBeak()
	TheDataMap = NewGlobalMap(NewGlobalData())

	TheOWLNode          *Owl
	TheKVServer         *KVServer
	TheClusterAPIServer *ClusterAPIServer
)

func JoinCluster(address, cluster string) {
	TheOWLNode = NewOwl(address, cluster, TheBeak, TheDataMap)
}

func StartKVServer(port string) {
	if TheOWLNode == nil {
		panic("OWL Node must be initialized first")
	}
	TheKVServer = NewKVServer(TheOWLNode, TheBeak, TheDataMap)
	srv := http.Server{
		Addr:    port,
		Handler: TheKVServer,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
}

func StartClusterAPIServer(port string) {
	if TheOWLNode == nil {
		panic("OWL Node must be initialized first")
	}
	TheClusterAPIServer = NewClusterAPIServer(TheOWLNode)
	srv := http.Server{
		Addr:    port,
		Handler: TheClusterAPIServer,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
}

func CleanValue(key string) {
	var (
		message = Message{
			Key:    key,
			Action: DeleteAction,
		}
	)

	msg, _ := json.Marshal(message)
	TheBeak.Eat(string(msg))
}

func SetValue(key, value string) {
	var (
		message = Message{
			Key:    key,
			Value:  value,
			Action: UpdateAction,
		}
	)
	msg, _ := json.Marshal(message)
	TheBeak.Eat(string(msg))
}

func GetValue(key string) (bool, string) {
	hit, value, _, _ := TheDataMap.Get(key)
	return hit, value
}

func IsHealthy() bool {
	if TheOWLNode.state == Leader {
		return TheOWLNode.isHealthy()
	}

	replyBody, err := TheOWLNode.sendClusterActionToLeader(ActionGetHealthStatus)
	if err != nil {
		glog.Errorf("failed to get health status, err: %v", err)
		return false
	}

	return replyBody.(bool)
}

func GetCurrentHealthyNodeNum() (int, error) {
	if TheOWLNode.state == Leader {
		return TheOWLNode.healthChecker.getHealthyNodesNum(), nil
	}

	replyBody, err := TheOWLNode.sendClusterActionToLeader(ActionGetHealthyNodeNum)
	if err != nil {
		return -1, err
	}

	return replyBody.(int), nil
}

// WaitForHealthy ...
func WaitForHealthy() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			if IsHealthy() {
				return
			}
			glog.Warning("Cluster not ready")
		}
	}
}
