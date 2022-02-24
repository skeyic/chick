package owluster

import "flag"

func EnableGlogForTesting() {
	flag.Set("logtostderr", "true")
	flag.Set("v", "10")
	flag.Parse()
}
