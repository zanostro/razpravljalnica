package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

func main() {
	mode := "demo"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	mb := pb.NewMessageBoardClient(conn)
	cp := pb.NewControlPlaneClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// shared setup
	u, err := mb.CreateUser(ctx, &pb.CreateUserRequest{Name: "ana"})
	if err != nil {
		log.Fatal("CreateUser:", err)
	}
	t, err := mb.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "prva tema"})
	if err != nil {
		log.Fatal("CreateTopic:", err)
	}

	if mode == "sub" {
		state, err := cp.GetClusterState(ctx, &emptypb.Empty{})
		if err != nil {
			log.Fatal("GetClusterState:", err)
		}
		fmt.Println("Cluster head/tail:", state.Head.Address, state.Tail.Address)

		subNode, err := mb.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
			UserId:  u.Id,
			TopicId: []int64{t.Id},
		})
		if err != nil {
			log.Fatal("GetSubscriptionNode:", err)
		}
		fmt.Println("SubNode:", subNode.Node.Address, "token=", subNode.SubscribeToken)

		stream, err := mb.SubscribeTopic(context.Background(), &pb.SubscribeTopicRequest{
			TopicId:         []int64{t.Id},
			UserId:          u.Id,
			FromMessageId:   0,
			SubscribeToken:  subNode.SubscribeToken,
		})
		if err != nil {
			log.Fatal("SubscribeTopic:", err)
		}
		fmt.Println("Subscribed. Waiting for events...")

		for {
			ev, err := stream.Recv()
			if err != nil {
				log.Fatal("stream recv:", err)
			}
			fmt.Printf("EVENT seq=%d op=%s topic=%d msg=%d text=%q likes=%d\n",
				ev.SequenceNumber, ev.Op.String(),
				ev.Message.TopicId, ev.Message.Id, ev.Message.Text, ev.Message.Likes)
		}
	}

	// demo mode: povzroči nekaj eventov
	posted, err := mb.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: t.Id,
		UserId:  u.Id,
		Text:    "hello world",
	})
	if err != nil {
		log.Fatal("PostMessage:", err)
	}

	_, err = mb.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   t.Id,
		MessageId: posted.Id,
		UserId:    u.Id,
	})
	if err != nil {
		log.Fatal("LikeMessage:", err)
	}

	_, err = mb.UpdateMessage(ctx, &pb.UpdateMessageRequest{
		TopicId:   t.Id,
		UserId:    u.Id,
		MessageId: posted.Id,
		Text:      "edited text",
	})
	if err != nil {
		log.Fatal("UpdateMessage:", err)
	}

	_, err = mb.DeleteMessage(ctx, &pb.DeleteMessageRequest{
		TopicId:   t.Id,
		UserId:    u.Id,
		MessageId: posted.Id,
	})
	if err != nil {
		log.Fatal("DeleteMessage:", err)
	}

	fmt.Println("demo done")
}
