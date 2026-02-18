package api

import (
	"net/http"

	commonmw "github.com/openjobspec/ojs-go-backend-common/middleware"
)

// MaxBodySize is the maximum allowed request body size (1 MB).
const MaxBodySize = 1 << 20

// OJSHeaders middleware adds required OJS response headers.
func OJSHeaders(next http.Handler) http.Handler {
	return commonmw.OJSHeaders(next)
}

// ValidateContentType middleware validates the Content-Type header for POST requests.
func ValidateContentType(next http.Handler) http.Handler {
	return commonmw.ValidateContentType(next)
}

// LimitRequestBody middleware limits the size of incoming request bodies.
func LimitRequestBody(next http.Handler) http.Handler {
	return commonmw.LimitRequestBody(next)
}
