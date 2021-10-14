package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/skeyic/chick/cluster"
	"net/http"
)

// Index ...
// @Summary	Index
// @Tags	index
// @Accept	json
// @Produce	json
// @Success 200	{string} string "WHO AM I"
// @Router / [get]
func Index(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Here is the Chick server, my node ID: " + cluster.NodeID,
	})
}

//NotFinished not implemented
func NotFinished(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "not implemented",
	})
}

//NotSupport ...
func NotSupport(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "not support",
	})
}
