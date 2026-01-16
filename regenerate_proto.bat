@echo off
REM Regenerate protobuf files for xeniq backend

echo Cleaning old generated files...
del /Q pb\*.pb.go 2>nul
del /Q pb\*_grpc.pb.go 2>nul

echo.
echo Regenerating from proto files...
REM Only generate from xeniq.proto - livecontrol.proto has duplicate types
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/xeniq.proto

REM Move generated files to pb/ directory
move proto\xeniq.pb.go pb\ >nul 2>&1
move proto\xeniq_grpc.pb.go pb\ >nul 2>&1

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ Proto files regenerated successfully
    echo.
    echo Generated files:
    dir /B pb\*.pb.go
    echo.
) else (
    echo.
    echo ❌ Proto generation failed
    echo.
    echo Make sure you have protoc installed:
    echo   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    echo   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    echo.
)
