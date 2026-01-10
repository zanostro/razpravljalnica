package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"log"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
	"github.com/zanostro/razpravljalnica/internal/chain"
	"github.com/zanostro/razpravljalnica/internal/config"
)

// ChainServer implements gRPC services for chain replication
type ChainServer struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedControlPlaneServer
	pb.UnimplementedChainReplicationServer

	chainNode *chain.Node
	cfg       *config.Config
	tokens    map[string]bool // subscription tokens
}

func NewChainServer(chainNode *chain.Node, cfg *config.Config) *ChainServer {
	return &ChainServer{
		chainNode: chainNode,
		cfg:       cfg,
		tokens:    make(map[string]bool),
	}
}

// Write operations - only allowed on HEAD
func (s *ChainServer) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	if !s.chainNode.IsHead() {
		return nil, fmt.Errorf("write operations only allowed on HEAD node")
	}

	log.Printf("[HEAD] CreateUser: %s", req.Name)

	// Get sequence number
	seqNum := s.chainNode.GetStore().GetNextSequenceNumber()

	// Serialize request
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Create operation
	op := &pb.ChainOperation{
		SequenceNumber: seqNum,
		OperationType:  "CreateUser",
		Payload:        payload,
	}

	// Apply locally and propagate through chain
	result, err := s.chainNode.ForwardOperation(ctx, op)
	if err != nil {
		return nil, fmt.Errorf("chain operation failed: %w", err)
	}

	if !result.Success {
		return nil, fmt.Errorf("operation failed: %s", result.ErrorMessage)
	}

	// Unmarshal response
	var user pb.User
	if err := proto.Unmarshal(result.Response, &user); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &user, nil
}

func (s *ChainServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if !s.chainNode.IsHead() {
		return nil, fmt.Errorf("write operations only allowed on HEAD node")
	}

	log.Printf("[HEAD] CreateTopic: %s", req.Name)

	seqNum := s.chainNode.GetStore().GetNextSequenceNumber()
	payload, _ := proto.Marshal(req)

	op := &pb.ChainOperation{
		SequenceNumber: seqNum,
		OperationType:  "CreateTopic",
		Payload:        payload,
	}

	result, err := s.chainNode.ForwardOperation(ctx, op)
	if err != nil {
		return nil, err
	}
	if !result.Success {
		return nil, fmt.Errorf("operation failed: %s", result.ErrorMessage)
	}

	var topic pb.Topic
	proto.Unmarshal(result.Response, &topic)
	return &topic, nil
}

func (s *ChainServer) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	if !s.chainNode.IsHead() {
		return nil, fmt.Errorf("write operations only allowed on HEAD node")
	}

	log.Printf("[HEAD] PostMessage: topic=%d user=%d", req.TopicId, req.UserId)

	seqNum := s.chainNode.GetStore().GetNextSequenceNumber()
	payload, _ := proto.Marshal(req)

	op := &pb.ChainOperation{
		SequenceNumber: seqNum,
		OperationType:  "PostMessage",
		Payload:        payload,
	}

	result, err := s.chainNode.ForwardOperation(ctx, op)
	if err != nil {
		return nil, err
	}
	if !result.Success {
		return nil, fmt.Errorf("operation failed: %s", result.ErrorMessage)
	}

	var msg pb.Message
	proto.Unmarshal(result.Response, &msg)


	return &msg, nil
}

func (s *ChainServer) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	if !s.chainNode.IsHead() {
		return nil, fmt.Errorf("write operations only allowed on HEAD node")
	}

	log.Printf("[HEAD] UpdateMessage: topic=%d msg=%d", req.TopicId, req.MessageId)

	seqNum := s.chainNode.GetStore().GetNextSequenceNumber()
	payload, _ := proto.Marshal(req)

	op := &pb.ChainOperation{
		SequenceNumber: seqNum,
		OperationType:  "UpdateMessage",
		Payload:        payload,
	}

	result, err := s.chainNode.ForwardOperation(ctx, op)
	if err != nil {
		return nil, err
	}
	if !result.Success {
		return nil, fmt.Errorf("operation failed: %s", result.ErrorMessage)
	}

	var msg pb.Message
	proto.Unmarshal(result.Response, &msg)


	return &msg, nil
}

func (s *ChainServer) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	if !s.chainNode.IsHead() {
		return nil, fmt.Errorf("write operations only allowed on HEAD node")
	}

	log.Printf("[HEAD] DeleteMessage: topic=%d msg=%d", req.TopicId, req.MessageId)

	seqNum := s.chainNode.GetStore().GetNextSequenceNumber()
	payload, _ := proto.Marshal(req)

	op := &pb.ChainOperation{
		SequenceNumber: seqNum,
		OperationType:  "DeleteMessage",
		Payload:        payload,
	}

	result, err := s.chainNode.ForwardOperation(ctx, op)
	if err != nil {
		return nil, err
	}
	if !result.Success {
		return nil, fmt.Errorf("operation failed: %s", result.ErrorMessage)
	}


	return &emptypb.Empty{}, nil
}

func (s *ChainServer) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	if !s.chainNode.IsHead() {
		return nil, fmt.Errorf("write operations only allowed on HEAD node")
	}

	log.Printf("[HEAD] LikeMessage: topic=%d msg=%d", req.TopicId, req.MessageId)

	seqNum := s.chainNode.GetStore().GetNextSequenceNumber()
	payload, _ := proto.Marshal(req)

	op := &pb.ChainOperation{
		SequenceNumber: seqNum,
		OperationType:  "LikeMessage",
		Payload:        payload,
	}

	result, err := s.chainNode.ForwardOperation(ctx, op)
	if err != nil {
		return nil, err
	}
	if !result.Success {
		return nil, fmt.Errorf("operation failed: %s", result.ErrorMessage)
	}

	var msg pb.Message
	proto.Unmarshal(result.Response, &msg)

	return &msg, nil
}

// Read operations - only allowed on TAIL
func (s *ChainServer) ListTopics(ctx context.Context, req *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	if !s.chainNode.IsTail() {
		return nil, fmt.Errorf("read operations only allowed on TAIL node")
	}

	log.Printf("[TAIL] ListTopics")

	topics, err := s.chainNode.GetStore().ListTopics()
	if err != nil {
		return nil, err
	}
	pbTopics := make([]*pb.Topic, len(topics))
	for i, t := range topics {
		pbTopics[i] = &pb.Topic{Id: t.ID, Name: t.Title}
	}

	return &pb.ListTopicsResponse{Topics: pbTopics}, nil
}

func (s *ChainServer) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	if !s.chainNode.IsTail() {
		return nil, fmt.Errorf("read operations only allowed on TAIL node")
	}

	log.Printf("[TAIL] GetMessages: topic=%d", req.TopicId)

	msgs, err := s.chainNode.GetStore().GetMessages(req.TopicId, req.FromMessageId, req.Limit)
	if err != nil {
		return nil, err
	}
	pbMsgs := make([]*pb.Message, len(msgs))
	for i, m := range msgs {
		pbMsgs[i] = &pb.Message{
			Id:      m.ID,
			TopicId: m.TopicID,
			UserId:  m.UserID,
			Text:    m.Text,
			Likes:   m.Likes,
		}
	}

	return &pb.GetMessagesResponse{Messages: pbMsgs}, nil
}

// Subscription management - load balanced across nodes
func (s *ChainServer) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if !s.chainNode.IsHead() {
		return nil, fmt.Errorf("subscription requests must go to HEAD")
	}

	log.Printf("[HEAD] GetSubscriptionNode for topics: %v", req.TopicId)

	var selectedNode *config.NodeConfig
	if len(req.TopicId) > 0 {
		hash := fnv.New32a()
		hash.Write([]byte(fmt.Sprintf("%d", req.TopicId[0])))
		nodeIndex := int(hash.Sum32()) % len(s.cfg.Chain.Nodes)
		selectedNode = &s.cfg.Chain.Nodes[nodeIndex]
	} else {
		selectedNode = s.cfg.GetHead()
	}

	token := generateToken()
	s.tokens[token] = true

	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &pb.NodeInfo{
			NodeId:  selectedNode.NodeID,
			Address: selectedNode.Address,
		},
	}, nil
}

func (s *ChainServer) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	log.Printf("[%s] SubscribeTopic: topics=%v token=%s", s.chainNode.GetNodeID(), req.TopicId, req.SubscribeToken)

	if req.SubscribeToken == "" {
		return fmt.Errorf("invalid subscription token")
	}

	subMgr := s.chainNode.GetSubManager()
	for _, tid := range req.TopicId {
		msgs, err := s.chainNode.GetStore().GetMessages(tid, req.FromMessageId, 1000000)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			ev := &pb.MessageEvent{
				SequenceNumber: subMgr.NextSeq(),
				Op:             pb.OpType_OP_POST,
				Message: &pb.Message{
					Id:      m.ID,
					TopicId: m.TopicID,
					UserId:  m.UserID,
					Text:    m.Text,
					Likes:   m.Likes,
				},
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}

	subID, ch := subMgr.Add(req.UserId, req.TopicId)
	defer subMgr.Remove(subID)

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

// Control plane
func (s *ChainServer) GetClusterState(ctx context.Context, req *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	head := s.cfg.GetHead()
	tail := s.cfg.GetTail()

	return &pb.GetClusterStateResponse{
		Head: &pb.NodeInfo{
			NodeId:  head.NodeID,
			Address: head.Address,
		},
		Tail: &pb.NodeInfo{
			NodeId:  tail.NodeID,
			Address: tail.Address,
		},
	}, nil
}

// Chain replication service implementation
func (s *ChainServer) ForwardOperation(ctx context.Context, op *pb.ChainOperation) (*pb.ChainOperationResult, error) {
	return s.chainNode.ForwardOperation(ctx, op)
}

// Helper function to generate subscription tokens
func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
