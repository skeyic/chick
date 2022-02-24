package owluster

import (
	"log"
	"net/http"
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
