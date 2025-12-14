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
	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	c := pb.NewMessageBoardClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	u, err := c.CreateUser(ctx, &pb.CreateUserRequest{Name: "ana"})
	if err != nil {
		log.Fatal("CreateUser:", err)
	}
	t, err := c.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "prva tema"})
	if err != nil {
		log.Fatal("CreateTopic:", err)
	}

	posted, err := c.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: t.Id,
		UserId:  u.Id,
		Text:    "hello world",
	})
	if err != nil {
		log.Fatal("PostMessage:", err)
	}

	_, err = c.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   t.Id,
		MessageId: posted.Id,
		UserId:    u.Id,
	})
	if err != nil {
		log.Fatal("LikeMessage:", err)
	}

	_, err = c.UpdateMessage(ctx, &pb.UpdateMessageRequest{
		TopicId:   t.Id,
		UserId:    u.Id,
		MessageId: posted.Id,
		Text:      "edited text",
	})
	if err != nil {
		log.Fatal("UpdateMessage:", err)
	}

	msgs, err := c.GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       t.Id,
		FromMessageId: 0,
		Limit:         10,
	})
	if err != nil {
		log.Fatal("GetMessages:", err)
	}

	fmt.Println("Topics:")
	list, _ := c.ListTopics(ctx, &emptypb.Empty{})
	for _, topic := range list.Topics {
		fmt.Printf("- %d: %s\n", topic.Id, topic.Name)
	}

	fmt.Println("Messages:")
	for _, m := range msgs.Messages {
		fmt.Printf("- #%d (u=%d) [%s] %s likes=%d\n", m.Id, m.UserId, m.CreatedAt.AsTime().Format(time.RFC3339), m.Text, m.Likes)
	}

	_, err = c.DeleteMessage(ctx, &pb.DeleteMessageRequest{
		TopicId:   t.Id,
		UserId:    u.Id,
		MessageId: posted.Id,
	})
	if err != nil {
		log.Fatal("DeleteMessage:", err)
	}

	// ponovno preberi messages po delete-u
	msgs2, err := c.GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       t.Id,
		FromMessageId: 0,
		Limit:         10,
	})
	if err != nil {
		log.Fatal("GetMessages after delete:", err)
	}

	fmt.Println("Messages after delete:")
	for _, m := range msgs2.Messages {
		fmt.Printf("- #%d (u=%d) [%s] %s likes=%d\n",
			m.Id, m.UserId, m.CreatedAt.AsTime().Format(time.RFC3339), m.Text, m.Likes)
	}

}
