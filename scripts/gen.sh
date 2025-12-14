#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$ROOT/api/proto"
OUT_DIR="$ROOT/gen/pb"

mkdir -p "$OUT_DIR"

# Najdi include pot, kjer so google/protobuf/*.proto
INC=""
for d in /usr/include /usr/local/include; do
  if [ -f "$d/google/protobuf/empty.proto" ]; then
    INC="$d"
    break
  fi
done

if [ -z "$INC" ]; then
  echo "ERROR: Ne najdem google/protobuf/empty.proto v /usr/include ali /usr/local/include"
  echo "Namig: na Debian/Ubuntu: sudo apt install protobuf-compiler"
  exit 1
fi

protoc -I "$PROTO_DIR" -I "$INC" \
  --go_out="$OUT_DIR" --go_opt=paths=source_relative \
  --go-grpc_out="$OUT_DIR" --go-grpc_opt=paths=source_relative \
  "$PROTO_DIR/razpravljalnica.proto"
