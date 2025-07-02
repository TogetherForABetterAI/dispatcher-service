// src/protocol/encoder.go
package protocol

import (
	"encoding/json"
	"fmt"
	"net"
)

func EncodeBatchMessage(conn net.Conn, msg *BatchMessage) error {
	// Convertir el struct a JSON
	encoded, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error serializing batch message: %w", err)
	}

	// Agregar newline como delimitador (opcional pero útil)
	encoded = append(encoded, '\n')

	// Escribir directamente al stream TCP
	if _, err := conn.Write(encoded); err != nil {
		return fmt.Errorf("error writing to connection: %w", err)
	}

	return nil
}
