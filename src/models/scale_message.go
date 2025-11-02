package models

// ScaleMessage represents a scaling request message
// to be sent to the scalability exchange.
type ScaleMessage struct {
	ReplicaType string `json:"replica_type"` // "dispatcher" in this case
}
