// src/protocol/encoder.go
package protocol

import (
	"encoding/json"
	"fmt"
	"net"
)

func EncodeBatchMessage(conn net.Conn, msg *BatchMessage) ([]byte, error) {
	// Convertir el struct a JSON
	encoded, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error serializing batch message: %w", err)
	}

	// Agregar newline como delimitador (opcional pero útil)
	encoded = append(encoded, '\n')

	return encoded
}
