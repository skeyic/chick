package router

import (
	"github.com/gin-gonic/gin"
	"github.com/skeyic/chick/app/controller"
	"github.com/skeyic/chick/config"
)

// InitRouter ...
func InitRouter() *gin.Engine {
	if config.Config.DebugMode {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.Default()

	// Basic
	r.GET("/", controller.Index)

	return r
}
