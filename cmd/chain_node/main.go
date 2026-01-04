package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
	"github.com/zanostro/razpravljalnica/internal/chain"
	"github.com/zanostro/razpravljalnica/internal/config"
	"github.com/zanostro/razpravljalnica/internal/server"
	"github.com/zanostro/razpravljalnica/internal/store"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	nodeID := flag.String("node", "", "Node ID from config (required)")
	flag.Parse()

	if *nodeID == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -node <node_id> [-config <path>]\n", os.Args[0])
		os.Exit(1)
	}

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Get node configuration
	nodeCfg := cfg.GetNode(*nodeID)
	if nodeCfg == nil {
		log.Fatalf("Node %s not found in configuration", *nodeID)
	}

	log.Printf("Starting node %s (%s) on %s", nodeCfg.NodeID, nodeCfg.Role, nodeCfg.Address)

	// Create store
	st := store.New()

	// Create chain node
	chainNode, err := chain.NewNode(cfg, *nodeID, st)
	if err != nil {
		log.Fatalf("Failed to create chain node: %v", err)
	}
	defer chainNode.Close()

	// Create server
	srv := server.NewChainServer(chainNode, cfg)

	// Start gRPC server
	lis, err := net.Listen("tcp", nodeCfg.Address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", nodeCfg.Address, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMessageBoardServer(grpcServer, srv)
	pb.RegisterControlPlaneServer(grpcServer, srv)
	pb.RegisterChainReplicationServer(grpcServer, srv)

	// Handle shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		grpcServer.GracefulStop()
	}()

	log.Printf("[%s] Node ready - Role: %s", nodeCfg.NodeID, nodeCfg.Role)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
