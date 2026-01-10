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

	headConn, _ := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer headConn.Close()
	headClient := pb.NewMessageBoardClient(headConn)

	tailConn, _ := grpc.Dial("127.0.0.1:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer tailConn.Close()
	tailClient := pb.NewMessageBoardClient(tailConn)

	fmt.Println("=== TEST: Write to HEAD, Read from TAIL ===\n")

	fmt.Println("STEP 1: Writing data to HEAD...")
	user, err := headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "TestUser"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  ✓ User created: ID=%d Name=%s\n", user.Id, user.Name)

	topic, err := headClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "TestTopic"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  ✓ Topic created: ID=%d Name=%s\n", topic.Id, topic.Name)

	msg, err := headClient.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Test message from HEAD",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  ✓ Message posted: ID=%d Text='%s'\n", msg.Id, msg.Text)

	time.Sleep(100 * time.Millisecond)

	fmt.Println("\nSTEP 2: Reading data from TAIL...")
	topics, err := tailClient.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  ✓ Topics found: %d\n", len(topics.Topics))

	messages, err := tailClient.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic.Id})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  ✓ Messages found: %d\n", len(messages.Messages))

	fmt.Println("\nSTEP 3: Verifying consistency...")
	if len(messages.Messages) > 0 && messages.Messages[0].Text == msg.Text {
		fmt.Printf("  ✅ SUCCESS: Data matches! '%s'\n", msg.Text)
		fmt.Println("\n🎉 Chain replication working correctly!")
	} else {
		fmt.Println("  ❌ FAIL: Data mismatch!")
	}
}
