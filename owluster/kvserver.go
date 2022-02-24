package owluster

import (
	"bytes"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type KVServer struct {
	owl     *Owl
	beak    *Beak
	dataMap *GlobalMap
}

func NewKVServer(owl *Owl, beak *Beak, dataMap *GlobalMap) *KVServer {
	return &KVServer{
		owl:     owl,
		beak:    beak,
		dataMap: dataMap,
	}
}

func (s *KVServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.owl.isHealthy() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write(bytes.NewBufferString("{\"Error\":\"Service is unavailable\"}").Bytes())
	}
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPost:
		msg, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		glog.V(4).Infof("EAT: %s", msg)
		s.beak.Eat(string(msg))

		w.WriteHeader(http.StatusOK)
		w.Write(bytes.NewBufferString("{\"Status\":\"OK\"}").Bytes())

	case http.MethodGet:
		key := strings.TrimPrefix(r.RequestURI, "/")
		hit, value, term, index := TheDataMap.Get(key)
		if !hit {
			w.WriteHeader(http.StatusNotFound)
			w.Write(bytes.NewBufferString(fmt.Sprintf(`{"Key": "%s", "ERROR": "NOT FOUND"}`, key)).Bytes())
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write(bytes.NewBufferString(fmt.Sprintf(`{"Key": "%s", "Value": "%s", "Term": %d, "Index": %d}`,
				key, value, term, index)).Bytes())
		}
	default:
		w.Header().Set("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodGet)
		//w.Header().Add("Allow", http.MethodPut)
		//w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
