package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type DeadLetterHandler = commonapi.DeadLetterHandler

func NewDeadLetterHandler(backend core.Backend) *DeadLetterHandler {
	return commonapi.NewDeadLetterHandler(backend)
}
