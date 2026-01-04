package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

func main() {
	// Connect to HEAD
	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	mb := pb.NewMessageBoardClient(conn)
	cp := pb.NewControlPlaneClient(conn)
	ctx := context.Background()

	// Test GetClusterState
	fmt.Println("=== Testing GetClusterState ===")
	state, err := cp.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatal("GetClusterState:", err)
	}
	fmt.Printf("✓ HEAD: %s @ %s\n", state.Head.NodeId, state.Head.Address)
	fmt.Printf("✓ TAIL: %s @ %s\n", state.Tail.NodeId, state.Tail.Address)

	// Create user
	u, err := mb.CreateUser(ctx, &pb.CreateUserRequest{Name: "sub-test-user"})
	if err != nil {
		log.Fatal("CreateUser:", err)
	}

	// Create multiple topics and test subscription distribution
	fmt.Println("\n=== Testing Subscription Distribution ===")
	topics := []string{"topic-a", "topic-b", "topic-c"}
	topicIDs := make([]int64, 0)

	for _, name := range topics {
		t, err := mb.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name})
		if err != nil {
			log.Fatal("CreateTopic:", err)
		}
		topicIDs = append(topicIDs, t.Id)
		fmt.Printf("Created topic: ID=%d Name=%s\n", t.Id, t.Name)

		// Get subscription node for this topic
		subNode, err := mb.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
			UserId:  u.Id,
			TopicId: []int64{t.Id},
		})
		if err != nil {
			log.Fatal("GetSubscriptionNode:", err)
		}
		fmt.Printf("  → Subscription assigned to: %s @ %s (token: %s...)\n",
			subNode.Node.NodeId, subNode.Node.Address, subNode.SubscribeToken[:8])
	}

	fmt.Println("\n✅ Subscription distribution test successful!")
	fmt.Println("Note: Subscriptions are distributed based on topic hash for load balancing")
}
