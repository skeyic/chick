package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/golang/glog"
	"sync"
	"time"
)

/*
The OWL Cluster maintains a map which may be read & write from each node.
*/

type message struct {
	Key string
	Val string
	Opt string
}

func newAddMessage(key, value string) message {
	return message{
		Key: key,
		Val: value,
		Opt: "ADD",
	}
}

func (m message) IsAdd() bool {
	return m.Opt == "ADD"
}

type owl struct {
	lock     *sync.RWMutex
	proposeC chan<- string
	data     map[string]string
}

func newOwl(proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *owl {
	o := &owl{
		lock:     &sync.RWMutex{},
		proposeC: proposeC,
		data:     make(map[string]string),
	}

	go o.readCommits(commitC, errorC)
	return o
}

func (o *owl) live() {
	go o.eat()
	go o.roar()
}

func (o *owl) eat() {
	ticker := time.NewTicker(7 * time.Second)
	for {
		select {
		case a := <-ticker.C:
			o.lock.Lock()
			k := a.Format("15:04:05")
			v := fmt.Sprintf("%d", a.Nanosecond())
			glog.V(4).Infof("Start propose")
			startTime := time.Now()
			o.Propose(k, v)
			glog.V(4).Infof("Finish propose using %d milliseconds", time.Now().Sub(startTime).Milliseconds())
			o.data[k] = v
			o.lock.Unlock()
		}
	}
}

func (o *owl) roar() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			o.lock.RLock()
			for k, v := range o.data {
				glog.V(4).Infof("K: %s, V: %s", k, v)
			}
			o.lock.RUnlock()
		}
	}
}

func (o *owl) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for theCommit := range commitC {
		if theCommit != nil {
			for _, data := range theCommit.data {
				var (
					dataKv message
				)
				dec := gob.NewDecoder(bytes.NewBufferString(data))
				if err := dec.Decode(&dataKv); err != nil {
					glog.Fatalf("Could not decode message: %s, error: %v", data, err)
				}
				o.lock.Lock()
				if dataKv.IsAdd() {
					o.data[dataKv.Key] = dataKv.Val
				}
				o.lock.Unlock()
			}
		}
		close(theCommit.applyDoneC)
	}

	if err, ok := <-errorC; ok {
		glog.Fatalf("Got error %v from the errorChannel", err)
	}
}

func (o *owl) Propose(k, v string) {
	var (
		buf bytes.Buffer
	)

	if err := gob.NewEncoder(&buf).Encode(newAddMessage(k, v)); err != nil {
		glog.Fatalf("Should never reach here! Encode k: %s, v: %s failed with error: %v",
			k, v, err)
	}

	o.proposeC <- buf.String()
}
