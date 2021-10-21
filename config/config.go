package config

import (
	"github.com/jinzhu/configor"
)

var Config = struct {
	DebugMode bool `default:"false" env:"DEBUG_MODE"`
	Port      int  `default:"7766" env:"PORT"`
	Cluster   struct {
		Port int `default:"10110" env:"CLUSTER_PORT"`
	}
}{}

func init() {
	if err := configor.Load(&Config); err != nil {
		panic(err)
	}
}
