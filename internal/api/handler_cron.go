package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type CronHandler = commonapi.CronHandler

func NewCronHandler(backend core.Backend) *CronHandler {
	return commonapi.NewCronHandler(backend)
}
