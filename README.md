# Live Control Backend

## Prerequisites
- Go 1.20+
- Protocol Buffers Compiler (`protoc`)
- Go Protobuf Plugins:
  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```

## Build Instructions
1.  **Generate Protobuf Code**:
    Navigate to the `backend` directory and run:
    ```bash
    protoc --proto_path=proto --go_out=pb --go_opt=paths=source_relative --go-grpc_out=pb --go-grpc_opt=paths=source_relative livecontrol.proto
    ```
    *Ensure the `pb` directory exists before running.*

2.  **Build Server**:
    ```bash
    go mod tidy
    go build -o server.exe main.go
    ```

## Running
1. **Start Server**:
    ```bash
    ./server.exe
    ```
    The server will listen on port `50051`.

2. **Run Verification Client**:
    Open a new terminal and run:
    ```bash
    go run cmd/testclient/main.go
    ```
    This will connect to the server, join a session as a CONSUMER, and send a test command.
