#!/bin/bash
set -e

# Delete the old generated code
sudo rm -rf src/pb/
mkdir -p src/pb

# Run the proto-gen container to regenerate the code
docker compose run --rm proto-gen 