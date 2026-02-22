package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type WorkflowHandler = commonapi.WorkflowHandler

func NewWorkflowHandler(backend core.Backend) *WorkflowHandler {
	return commonapi.NewWorkflowHandler(backend)
}
