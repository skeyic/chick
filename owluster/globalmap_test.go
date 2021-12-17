package owluster

import (
	"encoding/json"
	"github.com/golang/glog"
	"github.com/skeyic/chick/utils"
	"testing"
)

func Test_GlobalMap(t *testing.T) {
	utils.EnableGlogForTesting()

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
	}
}
