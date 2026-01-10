package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

const (
	numSubscribers = 10
	numMessages    = 20
)

func main() {
	headAddr := flag.String("head", "localhost:50051", "HEAD node address")
	flag.Parse()

	ctx := context.Background()

	headConn, err := grpc.Dial(*headAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer headConn.Close()

	cpClient := pb.NewControlPlaneClient(headConn)
	state, err := cpClient.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatalf("GetClusterState failed: %v", err)
	}

	log.Printf("Connected to chain: HEAD=%s TAIL=%s", state.Head.Address, state.Tail.Address)

	headClient := pb.NewMessageBoardClient(headConn)

	user, err := headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "test_user"})
	if err != nil {
		log.Fatalf("CreateUser failed: %v", err)
	}
	log.Printf("Created user: %s (id=%d)", user.Name, user.Id)

	topic, err := headClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "test_topic"})
	if err != nil {
		log.Fatalf("CreateTopic failed: %v", err)
	}
	log.Printf("Created topic: %s (id=%d)", topic.Name, topic.Id)

	var wg sync.WaitGroup
	var totalReceived atomic.Int64
	subscriberStats := make([]int, numSubscribers)
	var statsMu sync.Mutex

	log.Printf("\n=== Starting %d subscribers ===", numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)
		subID := i
		go func() {
			defer wg.Done()

			subNodeResp, err := headClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
				UserId:  user.Id,
				TopicId: []int64{topic.Id},
			})
			if err != nil {
				log.Printf("Subscriber %d: GetSubscriptionNode failed: %v", subID, err)
				return
			}

			log.Printf("Subscriber %d assigned to node: %s (%s)", subID, subNodeResp.Node.NodeId, subNodeResp.Node.Address)

			subConn, err := grpc.Dial(subNodeResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Subscriber %d: Dial failed: %v", subID, err)
				return
			}
			defer subConn.Close()

			subClient := pb.NewMessageBoardClient(subConn)
			stream, err := subClient.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{
				TopicId:        []int64{topic.Id},
				UserId:         user.Id,
				FromMessageId:  0,
				SubscribeToken: subNodeResp.SubscribeToken,
			})
			if err != nil {
				log.Printf("Subscriber %d: SubscribeTopic failed: %v", subID, err)
				return
			}

			log.Printf("Subscriber %d: Connected and listening...", subID)

			count := 0
			timeout := time.After(10 * time.Second)
			for {
				select {
				case <-timeout:
					statsMu.Lock()
					subscriberStats[subID] = count
					statsMu.Unlock()
					log.Printf("Subscriber %d: Timeout - received %d messages", subID, count)
					return
				default:
					err := stream.RecvMsg(new(pb.MessageEvent))
					if err != nil {
						statsMu.Lock()
						subscriberStats[subID] = count
						statsMu.Unlock()
						log.Printf("Subscriber %d: Stream ended - received %d messages (err: %v)", subID, count, err)
						return
					}
					count++
					totalReceived.Add(1)

					if count >= numMessages {
						statsMu.Lock()
						subscriberStats[subID] = count
						statsMu.Unlock()
						log.Printf("Subscriber %d: Received all %d messages ✓", subID, count)
						return
					}
				}
			}
		}()
	}

	time.Sleep(2 * time.Second)

	log.Printf("\n=== Publishing %d messages ===", numMessages)
	for i := 0; i < numMessages; i++ {
		msg, err := headClient.PostMessage(ctx, &pb.PostMessageRequest{
			TopicId: topic.Id,
			UserId:  user.Id,
			Text:    fmt.Sprintf("Test message %d", i+1),
		})
		if err != nil {
			log.Printf("PostMessage %d failed: %v", i+1, err)
			continue
		}
		log.Printf("Published message %d (id=%d): %s", i+1, msg.Id, msg.Text)
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("\n=== Waiting for subscribers to receive messages ===")
	time.Sleep(3 * time.Second)

	log.Printf("Stopping subscribers...")

	wg.Wait()

	log.Printf("\n=== Results ===")
	log.Printf("Total messages published: %d", numMessages)
	log.Printf("Total messages received by all subscribers: %d", totalReceived.Load())
	log.Printf("\nPer-subscriber statistics:")

	statsMu.Lock()
	allReceived := true
	for i, count := range subscriberStats {
		status := "✓"
		if count < numMessages {
			status = "✗"
			allReceived = false
		}
		log.Printf("  Subscriber %2d: %2d messages %s", i, count, status)
	}
	statsMu.Unlock()

	if allReceived {
		log.Printf("\n✓ SUCCESS: All subscribers received all messages!")
	} else {
		log.Printf("\n✗ FAILURE: Some subscribers did not receive all messages")
	}
}
