package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

func main() {
	conn, err := grpc.Dial(
		"127.0.0.1:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	c := pb.NewMessageBoardClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1) Create user
	u, err := c.CreateUser(ctx, &pb.CreateUserRequest{Username: "ana"})
	if err != nil {
		log.Fatal("CreateUser:", err)
	}
	fmt.Println("Created user:", u.User.UserId, u.User.Username)

	// 2) Create topic
	t, err := c.CreateTopic(ctx, &pb.CreateTopicRequest{Title: "prva tema"})
	if err != nil {
		log.Fatal("CreateTopic:", err)
	}
	fmt.Println("Created topic:", t.Topic.TopicId, t.Topic.Title)

	// 3) List topics
	list, err := c.ListTopics(ctx, &pb.ListTopicsRequest{})
	if err != nil {
		log.Fatal("ListTopics:", err)
	}

	fmt.Println("Topics:")
	for _, topic := range list.Topics {
		fmt.Printf("- %d: %s\n", topic.TopicId, topic.Title)
	}
}
