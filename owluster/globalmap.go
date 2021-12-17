package owluster

import (
	"encoding/json"
	"github.com/golang/glog"
	"sync"
)

type GlobalMap struct {
	Data map[string]string
	Lock *sync.RWMutex
}

func NewGlobalMap() *GlobalMap {
	return &GlobalMap{
		Data: make(map[string]string),
		Lock: &sync.RWMutex{},
	}
}

func (g *GlobalMap) Zip() []byte {
	body, _ := json.Marshal(g)
	return body
}

type Message struct {
	Key    string
	Value  string
	Action string
}

const (
	AddAction    = "add"
	UpdateAction = "update"
	DeleteAction = "delete"
)

// Do ...
// msg examples:
func (g *GlobalMap) Do(msg string) {
	var (
		message = new(Message)
	)
	err := json.Unmarshal([]byte(msg), &message)
	if err != nil {
		glog.Errorf("failed to unmarshal msg: %s, error: %v", msg, err)
		return
	}

	glog.V(4).Infof("MSG: %+v", message)

	switch message.Action {
	case AddAction, UpdateAction:
		g.Lock.Lock()
		g.Data[message.Key] = message.Value
		g.Lock.Unlock()
	case DeleteAction:
		g.Lock.Lock()
		delete(g.Data, message.Key)
		g.Lock.Unlock()
	default:
		glog.Errorf("Unknown action: %s, msg: %s", message.Action, msg)
	}
}
