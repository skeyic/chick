package owluster

import (
	"encoding/json"
	"github.com/golang/glog"
	"testing"
)

func Test_GlobalMap(t *testing.T) {
	EnableGlogForTesting()

	var (
		m1 = Message{
			Key:    "a",
			Value:  "1",
			Action: AddAction,
		}

		m2 = Message{
			Key:    "a",
			Value:  "2",
			Action: AddAction,
		}

		m3 = Message{
			Key:    "b",
			Value:  "3",
			Action: AddAction,
		}

		m4 = Message{
			Key:    "b",
			Value:  "",
			Action: DeleteAction,
		}
	)
	for _, msg := range []Message{m1, m2, m3, m4} {
		m, _ := json.Marshal(msg)
		glog.V(4).Infof("%s", m)
		var (
			message = new(Message)
		)
		err := json.Unmarshal(m, &message)
		if err != nil {
			glog.Errorf("failed to unmarshal msg: %s, error: %v", msg, err)
			return
		}
		glog.V(4).Infof("MSG: %s, MESSAGE: %+v", m, message)
	}
	/*
		failed to unmarshal msg: '{Key:a,Value:1,Action:add}', error: invalid character '\'' looking for beginning of value
	*/
}
