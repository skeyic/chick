package owluster

import "flag"

const (
	RPCPort = ""
)

func EnableGlogForTesting() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "4")
	flag.Parse()
}
