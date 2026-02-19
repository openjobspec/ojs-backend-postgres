package core

import (
	common "github.com/openjobspec/ojs-go-backend-common/core"
)

// Event type aliases from shared package.
const (
	EventJobStateChanged = common.EventJobStateChanged
	EventJobProgress     = common.EventJobProgress
	EventServerShutdown  = common.EventServerShutdown
)

// Type aliases for event types.
type JobEvent = common.JobEvent
type EventPublisher = common.EventPublisher
type EventSubscriber = common.EventSubscriber

// Function aliases for event helpers.
var NewStateChangedEvent = common.NewStateChangedEvent
