package protocol


type BatchMessage struct {
    BatchID   int           `json:"batch_id"`
    BatchData []interface{} `json:"batch_data"` // usar tipo concreto según datos
    ClientID  string        `json:"client_id"`
    EOF       bool          `json:"EOF"`
}