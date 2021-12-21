package owluster

import (
	"bytes"
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

func responseOK(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	body := bytes.NewBufferString("{\"Status\":\"OK\"}")
	w.Write(body.Bytes())
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
		glog.V(4).Infof("EAT: %s", msg)
		s.beak.Eat(string(msg))
		responseOK(w)
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
