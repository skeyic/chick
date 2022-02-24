package owluster

import "flag"

const (
	RPCPort = ""
)

func EnableGlogForTesting() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "10")
	flag.Parse()
}
