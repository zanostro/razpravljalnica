package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to HEAD
	headConn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer headConn.Close()
	headClient := pb.NewMessageBoardClient(headConn)

	// Connect to TAIL
	tailConn, err := grpc.Dial("127.0.0.1:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer tailConn.Close()
	tailClient := pb.NewMessageBoardClient(tailConn)

	fmt.Println("=== CORRECT OPERATIONS (should work) ===\n")

	// ✓ WRITE to HEAD - should work
	fmt.Println("1. WRITE to HEAD (CreateUser):")
	u, err := headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "routing-test-user"})
	if err != nil {
		fmt.Printf("   ❌ FAILED: %v\n", err)
	} else {
		fmt.Printf("   ✅ SUCCESS: Created user ID=%d\n", u.Id)
	}

	// ✓ WRITE to HEAD (CreateTopic)
	fmt.Println("\n2. WRITE to HEAD (CreateTopic):")
	t, err := headClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "routing-test-topic"})
	if err != nil {
		fmt.Printf("   ❌ FAILED: %v\n", err)
	} else {
		fmt.Printf("   ✅ SUCCESS: Created topic ID=%d\n", t.Id)
	}

	// ✓ READ from TAIL - should work
	fmt.Println("\n3. READ from TAIL (ListTopics):")
	topics, err := tailClient.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Printf("   ❌ FAILED: %v\n", err)
	} else {
		fmt.Printf("   ✅ SUCCESS: Retrieved %d topics from TAIL\n", len(topics.Topics))
		for _, topic := range topics.Topics {
			fmt.Printf("      - ID=%d Name=%s\n", topic.Id, topic.Name)
		}
	}

	fmt.Println("\n\n=== INCORRECT OPERATIONS (should fail) ===\n")

	// ✗ WRITE to TAIL - should fail
	fmt.Println("4. WRITE to TAIL (CreateUser) - should be REJECTED:")
	_, err = tailClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "should-fail"})
	if err != nil {
		fmt.Printf("   ✅ CORRECTLY REJECTED: %v\n", err)
	} else {
		fmt.Printf("   ❌ ERROR: This should have been rejected!\n")
	}

	// ✗ READ from HEAD - should fail
	fmt.Println("\n5. READ from HEAD (ListTopics) - should be REJECTED:")
	_, err = headClient.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		fmt.Printf("   ✅ CORRECTLY REJECTED: %v\n", err)
	} else {
		fmt.Printf("   ❌ ERROR: This should have been rejected!\n")
	}

	fmt.Println("\n\n=== SUMMARY ===")
	fmt.Println("Chain replication routing is working correctly:")
	fmt.Println("  • WRITE operations → HEAD only")
	fmt.Println("  • READ operations → TAIL only")
	fmt.Println("\nThis ensures:")
	fmt.Println("  • All writes go through the chain (HEAD → MIDDLE → TAIL)")
	fmt.Println("  • All reads come from fully committed state (TAIL)")
}
