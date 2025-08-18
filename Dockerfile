FROM golang:1.23.10 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download


RUN apt-get update && apt-get install -y unzip wget \
  && wget https://github.com/protocolbuffers/protobuf/releases/download/v31.1/protoc-31.1-linux-x86_64.zip \
  && unzip protoc-31.1-linux-x86_64.zip -d /usr/local \
  && rm protoc-31.1-linux-x86_64.zip \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

  
  
COPY . .

# RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest


# RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# CMD ["protoc", "-I./src/proto", "--go_out=paths=source_relative:/app/src/pb", "--go-grpc_out=paths=source_relative:/app/src/pb", "./src/proto/new-client-service.proto"]

WORKDIR /app/src
RUN go build -o app-binary main.go

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=builder /app/src/app-binary .

EXPOSE 8080
CMD ["./app-binary"]