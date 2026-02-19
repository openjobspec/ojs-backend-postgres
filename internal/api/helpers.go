package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// requestToJob wraps the common RequestToJob for backward compatibility.
var requestToJob = commonapi.RequestToJob

// decodeBody reads the request body and unmarshals it into v.
func decodeBody(r *http.Request, v any) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}

// resolveRelativeTime converts a relative ISO 8601 duration prefixed with "+"
// (e.g., "+PT5S") into an absolute RFC3339 timestamp by adding the duration to
// the current time. If the value is not a relative format, it is returned as-is.
func resolveRelativeTime(value string) string {
	if strings.HasPrefix(value, "+PT") {
		durStr := value[1:]
		d, err := core.ParseISO8601Duration(durStr)
		if err == nil {
			return time.Now().Add(d).UTC().Format(time.RFC3339)
		}
	}
	return value
}
