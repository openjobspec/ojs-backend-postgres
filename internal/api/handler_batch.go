package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type BatchHandler = commonapi.BatchHandler

func NewBatchHandler(backend core.Backend) *BatchHandler {
	return commonapi.NewBatchHandler(backend)
}
