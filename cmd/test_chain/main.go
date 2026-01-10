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
	// Connect to HEAD node
	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	mb := pb.NewMessageBoardClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create user
	fmt.Println("Creating user...")
	u, err := mb.CreateUser(ctx, &pb.CreateUserRequest{Name: "test-user"})
	if err != nil {
		log.Fatal("CreateUser:", err)
	}
	fmt.Printf("✓ User created: ID=%d Name=%s\n", u.Id, u.Name)

	// Create topic
	fmt.Println("Creating topic...")
	t, err := mb.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "test-topic"})
	if err != nil {
		log.Fatal("CreateTopic:", err)
	}
	fmt.Printf("✓ Topic created: ID=%d Name=%s\n", t.Id, t.Name)

	// Post message
	fmt.Println("Posting message...")
	msg, err := mb.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: t.Id,
		UserId:  u.Id,
		Text:    "Hello from chain replication!",
	})
	if err != nil {
		log.Fatal("PostMessage:", err)
	}
	fmt.Printf("✓ Message posted: ID=%d Text=%s\n", msg.Id, msg.Text)

	// Now test reading from TAIL
	fmt.Println("\n--- Testing read from TAIL node ---")

	// Connect to TAIL node
	tailConn, err := grpc.Dial("127.0.0.1:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer tailConn.Close()

	mbTail := pb.NewMessageBoardClient(tailConn)

	// List topics
	fmt.Println("Listing topics from TAIL...")
	topics, err := mbTail.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatal("ListTopics:", err)
	}
	fmt.Printf("✓ Topics from TAIL: %d topics\n", len(topics.Topics))
	for _, topic := range topics.Topics {
		fmt.Printf("  - ID=%d Name=%s\n", topic.Id, topic.Name)
	}

	// Get messages
	fmt.Println("Getting messages from TAIL...")
	messages, err := mbTail.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: t.Id})
	if err != nil {
		log.Fatal("GetMessages:", err)
	}
	fmt.Printf("✓ Messages from TAIL: %d messages\n", len(messages.Messages))
	for _, m := range messages.Messages {
		fmt.Printf("  - ID=%d Text=%s Likes=%d\n", m.Id, m.Text, m.Likes)
	}

	fmt.Println("\n✅ Chain replication test successful!")
}
