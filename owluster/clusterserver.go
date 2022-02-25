package owluster

import (
	"bytes"
	"fmt"
	"net/http"
)

type ClusterAPIServer struct {
	owl *Owl
}

func NewClusterAPIServer(owl *Owl) *ClusterAPIServer {
	return &ClusterAPIServer{
		owl: owl,
	}
}

func ServiceUnavailableError(w http.ResponseWriter) {
	w.WriteHeader(http.StatusServiceUnavailable)
	w.Write(bytes.NewBufferString("{\"Error\":\"Service is unavailable\"}").Bytes())
}

func (s *ClusterAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.owl.isHealthy() {
		ServiceUnavailableError(w)
		return
	}

	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		switch r.RequestURI {
		case "/health":
			{
				var health = "FALSE"
				if IsHealthy() {
					health = "TRUE"
				}
				w.WriteHeader(http.StatusOK)
				w.Write(bytes.NewBufferString(fmt.Sprintf(`{"Health": "%s"}`, health)).Bytes())
			}
		case "/master":
			{
				w.WriteHeader(http.StatusOK)
				w.Write(bytes.NewBufferString(fmt.Sprintf(`{"Master": "%s"}`, s.owl.leaderAddress)).Bytes())
			}
		case "/nodes":
			{
				if s.owl.state != Leader {
					w.WriteHeader(http.StatusOK)
					w.Write(bytes.NewBufferString(fmt.Sprintf(`{"ERROR": "Please query master node %s"}`, s.owl.leaderAddress)).Bytes())
				} else {
					nodes := s.owl.getNodesStatus()
					var (
						result    = bytes.NewBufferString("[")
						firstNode = true
					)
					for _, node := range nodes {
						node.lock.RLock()

						if !firstNode {
							result.WriteString(", ")
						} else {
							firstNode = false
						}

						result.WriteString(fmt.Sprintf(`{"Node": "%s", "Health": %v, Term": %d, "Log_Index": %d, "Data_Processing": %v}`,
							node.node, s.owl.healthChecker.isNodeHealthy(node.node), node.term, node.logIndex, node.dataProcessing))

						node.lock.RUnlock()
					}
					result.WriteString("]")
					w.WriteHeader(http.StatusOK)
					w.Write(result.Bytes())
				}
			}
		case "/healthy_nodes_num":
			{
				num, err := GetCurrentHealthyNodeNum()
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write(bytes.NewBufferString(fmt.Sprintf(`{"ERROR": "%v"`, err)).Bytes())
				} else {
					w.WriteHeader(http.StatusOK)
					w.Write(bytes.NewBufferString(fmt.Sprintf(`{"Healthy_Nodes_Num": %d}`, num)).Bytes())
				}
			}
		default:
			{
				w.WriteHeader(http.StatusBadRequest)
				w.Write(bytes.NewBufferString(`{"ERROR": "Only support /health, /master, /nodes, healthy_nodes_num"`).Bytes())
			}
		}

	default:
		//w.Header().Set("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodGet)
		//w.Header().Add("Allow", http.MethodPut)
		//w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
