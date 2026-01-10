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
	conn, err := grpc.Dial("127.0.0.1:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	mb := pb.NewMessageBoardClient(conn)
	cp := pb.NewControlPlaneClient(conn)

	ctx := context.Background()

	// setup: user + topic
	u, err := mb.CreateUser(ctx, &pb.CreateUserRequest{Name: "sub-user"})
	if err != nil {
		log.Fatal(err)
	}
	t, err := mb.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "sub-topic"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("SUB TOPIC ID:", t.Id)

	// cluster state (ni nujno, samo za test)
	state, err := cp.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cluster:", state.Head.Address)

	// get subscription node
	subNode, err := mb.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
		UserId:  u.Id,
		TopicId: []int64{t.Id},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Sub node:", subNode.Node.Address, "token:", subNode.SubscribeToken)

	// subscribe
	stream, err := mb.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{
		UserId:         u.Id,
		TopicId:        []int64{t.Id},
		FromMessageId:  0,
		SubscribeToken: subNode.SubscribeToken,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("SUBSCRIBED – waiting for events")

	for {
		ev, err := stream.Recv()
		if err != nil {
			log.Fatal("stream error:", err)
		}

		fmt.Printf(
			"EVENT seq=%d op=%s topic=%d msg=%d user=%d text=%q likes=%d\n",
			ev.SequenceNumber,
			ev.Op.String(),
			ev.Message.TopicId,
			ev.Message.Id,
			ev.Message.UserId,
			ev.Message.Text,
			ev.Message.Likes,
		)
	}
}
