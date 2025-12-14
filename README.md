# Razpravljalnica

## Build
```bash
go build ./...
```

## Run
### Server
```bash
go run ./cmd/server
```
### Client
```bash
go run ./cmd/client
```

## Struktura
- api/proto/ – gRPC definicija

- gen/pb/ – generirana gRPC koda

- internal/store/ – poslovna logika + in-memory storage

- internal/server/ – gRPC handlerji

- cmd/server/ – zagon strežnika

- cmd/client/ – preprost testni klient

## Dev
- Go: >= 1.21
