package models

// ConnectNotification represents the message structure from the auth gateway
type ConnectNotification struct {
	UserID                string `json:"user_id"`
	SessionId             string `json:"session_id"`
	TotalBatchesGenerated int    `json:"total_batches_generated"`
}
