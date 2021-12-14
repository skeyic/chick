package owluster

import (
	"github.com/golang/glog"
	"io/ioutil"
	"log"
	"net/http"
)

type Server struct {
	beak *Beak
}

func NewServer(beak *Beak) *Server {
	return &Server{beak: beak}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//key := r.RequestURI
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPost:
		msg, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		glog.V(4).Infof("MSG: %s", msg)
		//glog.V(4).Infof("CHEW: %s, ERROR: %+v", msg, s.beak.Chew(string(msg)))
		glog.V(4).Infof("EAT: %s", msg)
		s.beak.Eat(string(msg))

		glog.V(4).Infof("FINISH: %s", msg)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", http.MethodPost)
		//w.Header().Add("Allow", http.MethodGet)
		//w.Header().Add("Allow", http.MethodPut)
		//w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ServeHttpKVAPI starts a server and listens
func ServeHttpKVAPI(port string, server *Server) {
	srv := http.Server{
		Addr:    port,
		Handler: server,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
}
