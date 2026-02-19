package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type QueueHandler = commonapi.QueueHandler

func NewQueueHandler(backend core.Backend) *QueueHandler {
	return commonapi.NewQueueHandler(backend)
}
