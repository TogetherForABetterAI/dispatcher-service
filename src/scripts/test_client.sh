#!/bin/bash
set -e

echo "Building test client..."
cd src/test_client
go build -o test_client main.go

echo "Running test client..."
./test_client 