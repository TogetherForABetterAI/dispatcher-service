module github.com/mlops-eval/data-dispatcher-service

go 1.23.0

toolchain go1.23.10

// Note: Run 'go mod tidy' after uncommenting dependencies to download them

require (
	github.com/joho/godotenv v1.5.1
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.11.1
	google.golang.org/grpc v1.73.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250324211829-b45e905df463 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
