package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type SSEHandler = commonapi.SSEHandler

func NewSSEHandler(backend core.Backend, subscriber core.EventSubscriber) *SSEHandler {
	return commonapi.NewSSEHandler(backend, subscriber)
}
