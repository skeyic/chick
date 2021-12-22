package owluster

import (
	"encoding/json"
	"github.com/golang/glog"
	"sync"
)

type GlobalMap struct {
	Data  map[string]string
	Lock  *sync.RWMutex
	Term  int
	Index int
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

func (g *GlobalMap) Unzip(body []byte) {
	err := json.Unmarshal(body, &g)
	if err != nil {
		panic(err)
	}
}

func (g *GlobalMap) Version() (int, int) {
	g.Lock.RLock()
	defer g.Lock.RUnlock()
	return g.Term, g.Index
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
func (g *GlobalMap) Do(log *LogEntry) {
	var (
		msg     = log.Msg
		term    = log.LogTerm
		index   = log.LogIndex
		message = new(Message)
	)
	defer func() { log.Done = true }()

	err := json.Unmarshal([]byte(msg), &message)
	if err != nil {
		glog.Errorf("failed to unmarshal msg: %s, error: %v", msg, err)
		return
	}

	glog.V(4).Infof("DO MSG: %+v", message)
	g.Lock.RLock()
	myTerm, myIndex := g.Term, g.Index
	g.Lock.RUnlock()
	if myTerm > term || (myTerm == term && myIndex >= index) {
		glog.V(10).Infof("No need to update, myTerm: %d, myIndex: %d, msg term: %d, msg index: %d",
			myTerm, myIndex, term, index)
		return
	}

	switch message.Action {
	case AddAction, UpdateAction:
		g.Lock.Lock()
		g.Data[message.Key] = message.Value
		g.Term = term
		g.Index = index
		g.Lock.Unlock()
	case DeleteAction:
		g.Lock.Lock()
		delete(g.Data, message.Key)
		g.Term = term
		g.Index = index
		g.Lock.Unlock()
	default:
		glog.Errorf("Unknown action: %s, msg: %s", message.Action, msg)
	}
}

// Report ...
func (g *GlobalMap) Report() {
	glog.V(4).Infof("GM REPORT: IDX: %d, TERM: %d", g.Index, g.Term)
	g.Lock.RLock()
	for key, value := range g.Data {
		glog.V(4).Infof("KEY: %s, VALUE: %s", key, value)
	}
	g.Lock.RUnlock()
}
