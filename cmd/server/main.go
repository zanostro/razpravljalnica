package main

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
	"github.com/zanostro/razpravljalnica/internal/server"
	"github.com/zanostro/razpravljalnica/internal/store"
)

func main() {
	addr := ":50051"

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	st := store.New()
	srv := server.New(st)

	grpcServer := grpc.NewServer()
	pb.RegisterMessageBoardServer(grpcServer, srv)
	pb.RegisterControlPlaneServer(grpcServer, srv)

	fmt.Println("server listening on", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
