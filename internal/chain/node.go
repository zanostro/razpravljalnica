package chain

import (
	"context"
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/zanostro/razpravljalnica/gen/pb"
	"github.com/zanostro/razpravljalnica/internal/config"
	"github.com/zanostro/razpravljalnica/internal/store"
)

// Node represents a node in the chain replication system
type Node struct {
	cfg           *config.Config
	nodeID        string
	role          config.NodeRole
	store         *store.Store
	successorAddr string
	
	// gRPC client connection to successor (if not TAIL)
	successorConn   *grpc.ClientConn
	successorClient pb.ChainReplicationClient
	
	// Pending acknowledgments (for HEAD and MIDDLE nodes)
	pendingAcks map[int64]chan *pb.ChainOperationResult
	ackMu       sync.RWMutex
}

// NewNode creates a new chain node
func NewNode(cfg *config.Config, nodeID string, st *store.Store) (*Node, error) {
	nodeCfg := cfg.GetNode(nodeID)
	if nodeCfg == nil {
		return nil, fmt.Errorf("node %s not found in config", nodeID)
	}

	n := &Node{
		cfg:         cfg,
		nodeID:      nodeID,
		role:        nodeCfg.Role,
		store:       st,
		pendingAcks: make(map[int64]chan *pb.ChainOperationResult),
	}

	// Connect to successor if not TAIL
	if nodeCfg.Role != config.RoleTail {
		successor := cfg.GetSuccessor(nodeID)
		if successor != nil {
			n.successorAddr = successor.Address
			if err := n.connectToSuccessor(); err != nil {
				return nil, fmt.Errorf("connect to successor: %w", err)
			}
		}
	}

	return n, nil
}

// connectToSuccessor establishes connection to the next node in chain
func (n *Node) connectToSuccessor() error {
	conn, err := grpc.Dial(n.successorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial successor: %w", err)
	}
	n.successorConn = conn
	n.successorClient = pb.NewChainReplicationClient(conn)
	log.Printf("[%s] Connected to successor at %s", n.nodeID, n.successorAddr)
	return nil
}

// Close closes connections
func (n *Node) Close() error {
	if n.successorConn != nil {
		return n.successorConn.Close()
	}
	return nil
}

// IsHead returns true if this node is the HEAD
func (n *Node) IsHead() bool {
	return n.role == config.RoleHead
}

// IsTail returns true if this node is the TAIL
func (n *Node) IsTail() bool {
	return n.role == config.RoleTail
}

// ForwardOperation implements ChainReplication service
// This handles operations forwarded from predecessor nodes
func (n *Node) ForwardOperation(ctx context.Context, op *pb.ChainOperation) (*pb.ChainOperationResult, error) {
	log.Printf("[%s] Received operation seq=%d type=%s", n.nodeID, op.SequenceNumber, op.OperationType)

	// Apply operation to local store
	result, err := n.applyOperation(ctx, op)
	if err != nil {
		log.Printf("[%s] Error applying operation: %v", n.nodeID, err)
		return &pb.ChainOperationResult{
			SequenceNumber: op.SequenceNumber,
			Success:        false,
			ErrorMessage:   err.Error(),
		}, nil
	}

	// If this is TAIL, operation is complete - send back result
	if n.IsTail() {
		log.Printf("[%s] TAIL node - operation complete, sending ACK", n.nodeID)
		return result, nil
	}

	// Otherwise, forward to successor and wait for ACK
	log.Printf("[%s] Forwarding to successor...", n.nodeID)
	ack, err := n.successorClient.ForwardOperation(ctx, op)
	if err != nil {
		return &pb.ChainOperationResult{
			SequenceNumber: op.SequenceNumber,
			Success:        false,
			ErrorMessage:   fmt.Sprintf("successor error: %v", err),
		}, nil
	}

	log.Printf("[%s] Received ACK from successor", n.nodeID)
	return ack, nil
}

// applyOperation applies the operation to local store
func (n *Node) applyOperation(ctx context.Context, op *pb.ChainOperation) (*pb.ChainOperationResult, error) {
	result := &pb.ChainOperationResult{
		SequenceNumber: op.SequenceNumber,
		Success:        true,
	}

	switch op.OperationType {
	case "CreateUser":
		var req pb.CreateUserRequest
		if err := proto.Unmarshal(op.Payload, &req); err != nil {
			return nil, fmt.Errorf("unmarshal CreateUserRequest: %w", err)
		}
		user, err := n.store.CreateUser(req.Name)
		if err != nil {
			return nil, err
		}
		resp := &pb.User{Id: user.ID, Name: user.Username}
		respData, _ := proto.Marshal(resp)
		result.Response = respData

	case "CreateTopic":
		var req pb.CreateTopicRequest
		if err := proto.Unmarshal(op.Payload, &req); err != nil {
			return nil, fmt.Errorf("unmarshal CreateTopicRequest: %w", err)
		}
		topic, err := n.store.CreateTopic(req.Name)
		if err != nil {
			return nil, err
		}
		resp := &pb.Topic{Id: topic.ID, Name: topic.Title}
		respData, _ := proto.Marshal(resp)
		result.Response = respData

	case "PostMessage":
		var req pb.PostMessageRequest
		if err := proto.Unmarshal(op.Payload, &req); err != nil {
			return nil, fmt.Errorf("unmarshal PostMessageRequest: %w", err)
		}
		msg, err := n.store.PostMessage(req.TopicId, req.UserId, req.Text)
		if err != nil {
			return nil, err
		}
		resp := n.storeMessageToPb(&msg)
		respData, _ := proto.Marshal(resp)
		result.Response = respData

	case "UpdateMessage":
		var req pb.UpdateMessageRequest
		if err := proto.Unmarshal(op.Payload, &req); err != nil {
			return nil, fmt.Errorf("unmarshal UpdateMessageRequest: %w", err)
		}
		msg, err := n.store.UpdateMessage(req.TopicId, req.MessageId, req.UserId, req.Text)
		if err != nil {
			return nil, err
		}
		resp := n.storeMessageToPb(&msg)
		respData, _ := proto.Marshal(resp)
		result.Response = respData

	case "DeleteMessage":
		var req pb.DeleteMessageRequest
		if err := proto.Unmarshal(op.Payload, &req); err != nil {
			return nil, fmt.Errorf("unmarshal DeleteMessageRequest: %w", err)
		}
		_, err := n.store.DeleteMessage(req.TopicId, req.MessageId, req.UserId)
		if err != nil {
			return nil, err
		}

	case "LikeMessage":
		var req pb.LikeMessageRequest
		if err := proto.Unmarshal(op.Payload, &req); err != nil {
			return nil, fmt.Errorf("unmarshal LikeMessageRequest: %w", err)
		}
		msg, err := n.store.LikeMessage(req.TopicId, req.MessageId, req.UserId)
		if err != nil {
			return nil, err
		}
		resp := n.storeMessageToPb(&msg)
		respData, _ := proto.Marshal(resp)
		result.Response = respData

	default:
		return nil, fmt.Errorf("unknown operation type: %s", op.OperationType)
	}

	return result, nil
}

// Helper to convert store message to pb message
func (n *Node) storeMessageToPb(msg *store.Message) *pb.Message {
	return &pb.Message{
		Id:      msg.ID,
		TopicId: msg.TopicID,
		UserId:  msg.UserID,
		Text:    msg.Text,
		Likes:   msg.Likes,
	}
}

// GetStore returns the underlying store
func (n *Node) GetStore() *store.Store {
	return n.store
}

// GetRole returns the node's role
func (n *Node) GetRole() config.NodeRole {
	return n.role
}

// GetNodeID returns the node's ID
func (n *Node) GetNodeID() string {
	return n.nodeID
}
