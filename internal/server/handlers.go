package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
	"github.com/zanostro/razpravljalnica/internal/store"
)

// MessageBoard RPCs

func (s *Server) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	u, err := s.store.CreateUser(req.GetName())
	if err != nil {
		return nil, grpcErr(err)
	}

	return &pb.User{
		Id:   u.ID,
		Name: u.Username,
	}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	t, err := s.store.CreateTopic(req.GetName())
	if err != nil {
		return nil, grpcErr(err)
	}

	return &pb.Topic{
		Id:   t.ID,
		Name: t.Title,
	}, nil
}

func (s *Server) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	m, err := s.store.PostMessage(req.GetTopicId(), req.GetUserId(), req.GetText())
	if err != nil {
		return nil, grpcErr(err)
	}

	pbMsg := &pb.Message{
		Id:        m.ID,
		TopicId:   m.TopicID,
		UserId:    m.UserID,
		Text:      m.Text,
		CreatedAt: timestamppb.New(m.CreatedAt),
		Likes:     m.Likes,
	}

	s.sub.Publish(m.TopicID, &pb.MessageEvent{
		SequenceNumber: s.sub.NextSeq(),
		Op:             pb.OpType_OP_POST,
		Message:        pbMsg,
		EventAt:        timestamppb.Now(),
	})

	return pbMsg, nil
}

func (s *Server) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	m, err := s.store.UpdateMessage(req.GetTopicId(), req.GetMessageId(), req.GetUserId(), req.GetText())
	if err != nil {
		return nil, grpcErr(err)
	}

	pbMsg := &pb.Message{
		Id:        m.ID,
		TopicId:   m.TopicID,
		UserId:    m.UserID,
		Text:      m.Text,
		CreatedAt: timestamppb.New(m.CreatedAt),
		Likes:     m.Likes,
	}

	s.sub.Publish(m.TopicID, &pb.MessageEvent{
		SequenceNumber: s.sub.NextSeq(),
		Op:             pb.OpType_OP_UPDATE,
		Message:        pbMsg,
		EventAt:        timestamppb.Now(),
	})

	return pbMsg, nil
}

func (s *Server) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	deleted, err := s.store.DeleteMessage(
		req.GetTopicId(),
		req.GetMessageId(),
		req.GetUserId(),
	)
	if err != nil {
		return nil, grpcErr(err)
	}

	s.sub.Publish(deleted.TopicID, &pb.MessageEvent{
		SequenceNumber: s.sub.NextSeq(),
		Op:             pb.OpType_OP_DELETE,
		Message: &pb.Message{
			Id:        deleted.ID,
			TopicId:   deleted.TopicID,
			UserId:    deleted.UserID,
			Text:      deleted.Text,
			CreatedAt: timestamppb.New(deleted.CreatedAt),
			Likes:     deleted.Likes,
		},
		EventAt: timestamppb.Now(),
	})

	return &emptypb.Empty{}, nil
}

func (s *Server) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	m, err := s.store.LikeMessage(req.GetTopicId(), req.GetMessageId(), req.GetUserId())
	if err != nil {
		return nil, grpcErr(err)
	}

	pbMsg := &pb.Message{
		Id:        m.ID,
		TopicId:   m.TopicID,
		UserId:    m.UserID,
		Text:      m.Text,
		CreatedAt: timestamppb.New(m.CreatedAt),
		Likes:     m.Likes,
	}

	s.sub.Publish(m.TopicID, &pb.MessageEvent{
		SequenceNumber: s.sub.NextSeq(),
		Op:             pb.OpType_OP_LIKE,
		Message:        pbMsg,
		EventAt:        timestamppb.Now(),
	})

	return pbMsg, nil
}

func (s *Server) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	_ = ctx

	topics, err := s.store.ListTopics()
	if err != nil {
		return nil, grpcErr(err)
	}

	out := make([]*pb.Topic, 0, len(topics))
	for _, t := range topics {
		out = append(out, &pb.Topic{
			Id:   t.ID,
			Name: t.Title,
		})
	}

	return &pb.ListTopicsResponse{Topics: out}, nil
}

func (s *Server) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	msgs, err := s.store.GetMessages(req.GetTopicId(), req.GetFromMessageId(), req.GetLimit())
	if err != nil {
		return nil, grpcErr(err)
	}

	out := make([]*pb.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, &pb.Message{
			Id:        m.ID,
			TopicId:   m.TopicID,
			UserId:    m.UserID,
			Text:      m.Text,
			CreatedAt: timestamppb.New(m.CreatedAt),
			Likes:     m.Likes,
		})
	}

	return &pb.GetMessagesResponse{Messages: out}, nil
}

func (s *Server) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	// Single-node
	return &pb.SubscriptionNodeResponse{
		SubscribeToken: "dummy",
		Node: &pb.NodeInfo{
			NodeId:  "local",
			Address: "127.0.0.1:50051",
		},
	}, nil
}

func (s *Server) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "nil request")
	}

	// single-node token
	if req.GetSubscribeToken() != "dummy" {
		return status.Error(codes.PermissionDenied, "bad token")
	}

	userID := req.GetUserId()
	topics := req.GetTopicId()
	fromID := req.GetFromMessageId()

	// catch-up: pošlji existing messages kot OP_POST
	for _, tid := range topics {
		msgs, err := s.store.GetMessages(tid, fromID, 1000000)
		if err != nil {
			return grpcErr(err)
		}
		for _, m := range msgs {
			ev := &pb.MessageEvent{
				SequenceNumber: s.sub.NextSeq(),
				Op:             pb.OpType_OP_POST,
				Message: &pb.Message{
					Id:        m.ID,
					TopicId:   m.TopicID,
					UserId:    m.UserID,
					Text:      m.Text,
					CreatedAt: timestamppb.New(m.CreatedAt),
					Likes:     m.Likes,
				},
				EventAt: timestamppb.Now(),
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}

	// live
	subID, ch := s.sub.Add(userID, topics)
	defer s.sub.Remove(subID)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

func (s *Server) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	_ = ctx

	ni := &pb.NodeInfo{
		NodeId:  "local",
		Address: "127.0.0.1:50051",
	}

	return &pb.GetClusterStateResponse{
		Head: ni,
		Tail: ni,
	}, nil
}

// Helper: map store errors -> gRPC codes (za kasneje)
func grpcErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrForbidden):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, store.ErrDuplicate):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrBadRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
