package models

// ConnectNotification represents the message structure from the auth gateway
type ConnectNotification struct {
	ClientId      string `json:"client_id"`
	InputsFormat  string `json:"inputs_format"`
	OutputsFormat string `json:"outputs_format"`
}
