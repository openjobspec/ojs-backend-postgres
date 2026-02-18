package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type WorkerHandler = commonapi.WorkerHandler

func NewWorkerHandler(backend core.Backend) *WorkerHandler {
	return commonapi.NewWorkerHandler(backend)
}
