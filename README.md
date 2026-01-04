# Razpravljalnica

## Chain Replication Setup (Grade 7-8)

This project implements chain replication for distributed message board operations:
- **Write operations** go to HEAD node
- **Read operations** go to TAIL node  
- **Subscriptions** are load-balanced across nodes

### Configuration

Edit `config.yaml` to define your chain nodes:

```yaml
chain:
  nodes:
    - node_id: "node1"
      address: "localhost:50051"
      role: "HEAD"
    - node_id: "node2"
      address: "localhost:50052"
      role: "MIDDLE"
    - node_id: "node3"
      address: "localhost:50053"
      role: "TAIL"
```

### Running Chain Replication

**Quick Start** - Use the automated script:

```bash
chmod +x start_chain.sh
./start_chain.sh
```

This will:
- Build all binaries
- Start all 3 chain nodes (HEAD, MIDDLE, TAIL)
- Show node PIDs and log locations

Then launch the GUI client:
```bash
./client
```

**Manual Start** - Start each node in a separate terminal:

```bash
# Build first
go build -o chain_node ./cmd/chain_node
go build -o client ./cmd/client

# Terminal 1 - TAIL node
./chain_node -node node3

# Terminal 2 - MIDDLE node
./chain_node -node node2

# Terminal 3 - HEAD node
./chain_node -node node1
```

**Stop all nodes:**
```bash
pkill chain_node
```

### Testing

Test the chain replication:
```bash
go run ./cmd/test_chain
```

This will:
1. Create a user via HEAD node
2. Create a topic via HEAD node
3. Post a message via HEAD node
4. Read topics from TAIL node
5. Read messages from TAIL node
6. Verify data consistency

### How It Works

1. **Write Path**: Client → HEAD → MIDDLE → TAIL → ACK back → HEAD → Client
2. **Read Path**: Client → TAIL (instant response from consistent state)
3. **Subscriptions**: HEAD assigns subscription to a node based on topic hash

### Architecture

- `internal/config/` - Configuration loading
- `internal/chain/` - Chain node implementation with ForwardOperation logic
- `internal/server/` - gRPC handlers that enforce HEAD/TAIL routing
- `cmd/chain_node/` - Main executable for running chain nodes

## Build
```bash
go build ./...
```

## Run (Single Node - Grade 6-7)
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
