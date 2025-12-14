package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run ./cmd/client_pub <topic_id>")
		os.Exit(1)
	}
	topicID, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		log.Fatal("bad topic_id:", err)
	}

	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	mb := pb.NewMessageBoardClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	u, err := mb.CreateUser(ctx, &pb.CreateUserRequest{Name: "pub-user"})
	if err != nil {
		log.Fatal("CreateUser:", err)
	}

	posted, err := mb.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topicID,
		UserId:  u.Id,
		Text:    "hello from publisher",
	})
	if err != nil {
		log.Fatal("PostMessage:", err)
	}

	_, err = mb.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   topicID,
		MessageId: posted.Id,
		UserId:    u.Id,
	})
	if err != nil {
		log.Fatal("LikeMessage:", err)
	}

	_, err = mb.UpdateMessage(ctx, &pb.UpdateMessageRequest{
		TopicId:   topicID,
		UserId:    u.Id,
		MessageId: posted.Id,
		Text:      "edited by publisher",
	})
	if err != nil {
		log.Fatal("UpdateMessage:", err)
	}

	_, err = mb.DeleteMessage(ctx, &pb.DeleteMessageRequest{
		TopicId:   topicID,
		UserId:    u.Id,
		MessageId: posted.Id,
	})
	if err != nil {
		log.Fatal("DeleteMessage:", err)
	}

	fmt.Println("publisher done")
}
