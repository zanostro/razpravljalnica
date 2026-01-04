#!/bin/bash

set -e

# Check if chain_node processes are already running
if pgrep -f "chain_node" > /dev/null; then
    echo "Error: chain_node processes are already running"
    echo ""
    echo "Running processes:"
    pgrep -a -f "chain_node"
    echo ""
    echo "Please stop them first:"
    echo "  pkill chain_node"
    exit 1
fi

# Check if required ports are available
for port in 50051 50052 50053; do
    if lsof -i :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "Error: Port $port is already in use"
        echo ""
        echo "Process using port $port:"
        lsof -i :$port -sTCP:LISTEN
        echo ""
        echo "Please stop the process using this port first"
        exit 1
    fi
done

echo "✓ Pre-flight checks passed"
echo ""

echo "=== Building chain replication system ==="
go build -o chain_node ./cmd/chain_node
go build -o client ./cmd/client
echo "✓ Build complete"

echo ""
echo "=== Starting chain nodes ==="

# Start nodes
echo "Starting TAIL node (node3) on :50053..."
./chain_node -node node3 > /tmp/node3.log 2>&1 &
NODE3_PID=$!

sleep 0.5

echo "Starting MIDDLE node (node2) on :50052..."
./chain_node -node node2 > /tmp/node2.log 2>&1 &
NODE2_PID=$!

sleep 0.5

echo "Starting HEAD node (node1) on :50051..."
./chain_node -node node1 > /tmp/node1.log 2>&1 &
NODE1_PID=$!

sleep 1

# Check if nodes are running
if pgrep -f "chain_node -node node1" > /dev/null && \
   pgrep -f "chain_node -node node2" > /dev/null && \
   pgrep -f "chain_node -node node3" > /dev/null; then
    echo ""
    echo "All nodes started successfully!"
    echo "  - HEAD:   localhost:50051 (PID: $NODE1_PID)"
    echo "  - MIDDLE: localhost:50052 (PID: $NODE2_PID)"
    echo "  - TAIL:   localhost:50053 (PID: $NODE3_PID)"
    echo ""
    echo "Logs: /tmp/node{1,2,3}.log"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Chain is running. Press 'q' to shutdown..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Wait for 'q' key
    while true; do
        read -n 1 -s key
        if [[ $key == "q" ]] || [[ $key == "Q" ]]; then
            echo ""
            echo "Shutting down nodes gracefully..."
            
            # Send SIGTERM for graceful shutdown
            kill -TERM $NODE1_PID 2>/dev/null && echo "  ✓ HEAD stopped"
            kill -TERM $NODE2_PID 2>/dev/null && echo "  ✓ MIDDLE stopped"
            kill -TERM $NODE3_PID 2>/dev/null && echo "  ✓ TAIL stopped"
            
            sleep 1
            
            # Force kill if still running
            kill -9 $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
            
            echo ""
            echo "✓ Chain shutdown complete"
            exit 0
        fi
    done
else
    echo ""
    echo "Error: Some nodes failed to start"
    echo "Check logs:"
    tail -20 /tmp/node1.log /tmp/node2.log /tmp/node3.log
    exit 1
fi
