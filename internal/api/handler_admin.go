package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

type AdminHandler = commonapi.AdminHandler

func NewAdminHandler(backend core.Backend) *AdminHandler {
	return commonapi.NewAdminHandler(backend)
}
