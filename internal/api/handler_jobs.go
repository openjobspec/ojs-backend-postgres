package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type JobHandler = commonapi.JobHandler

func NewJobHandler(backend core.Backend) *JobHandler {
	return commonapi.NewJobHandler(backend)
}

var RequestToJob = commonapi.RequestToJob
