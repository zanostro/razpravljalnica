package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
	"github.com/zanostro/razpravljalnica/internal/store"
)

// MessageBoard RPCs

func (s *Server) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.CreateUserResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	u, err := s.store.CreateUser(req.GetUsername())
	if err != nil {
		return nil, grpcErr(err)
	}

	return &pb.CreateUserResponse{
		User: &pb.User{
			UserId:   u.ID,
			Username: u.Username,
		},
	}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	t, err := s.store.CreateTopic(req.GetTitle())
	if err != nil {
		return nil, grpcErr(err)
	}

	return &pb.CreateTopicResponse{
		Topic: &pb.Topic{
			TopicId: t.ID,
			Title:   t.Title,
		},
	}, nil
}

func (s *Server) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_, _ = ctx, req
	return nil, status.Error(codes.Unimplemented, "PostMessage not implemented yet")
}

func (s *Server) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_, _ = ctx, req
	return nil, status.Error(codes.Unimplemented, "UpdateMessage not implemented yet")
}

func (s *Server) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*pb.Message, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_, _ = ctx, req
	return nil, status.Error(codes.Unimplemented, "DeleteMessage not implemented yet")
}

func (s *Server) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_, _ = ctx, req
	return nil, status.Error(codes.Unimplemented, "LikeMessage not implemented yet")
}

func (s *Server) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_ = ctx

	topics, err := s.store.ListTopics()
	if err != nil {
		return nil, grpcErr(err)
	}

	out := make([]*pb.Topic, 0, len(topics))
	for _, t := range topics {
		out = append(out, &pb.Topic{
			TopicId: t.ID,
			Title:   t.Title,
		})
	}

	return &pb.ListTopicsResponse{Topics: out}, nil
}

func (s *Server) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_, _ = ctx, req
	return nil, status.Error(codes.Unimplemented, "GetMessages not implemented yet")
}

func (s *Server) GetSubscriptionNode(ctx context.Context, req *pb.GetSubscriptionNodeRequest) (*pb.GetSubscriptionNodeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	_, _ = ctx, req
	return nil, status.Error(codes.Unimplemented, "GetSubscriptionNode not implemented yet")
}

func (s *Server) SubscribeTopic(req *pb.SubscribeRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "nil request")
	}
	_ = stream
	return status.Error(codes.Unimplemented, "SubscribeTopic not implemented yet")
}

// ControlPlane RPCs

func (s *Server) GetHead(ctx context.Context, _ *emptypb.Empty) (*pb.NodeInfo, error) {
	_ = ctx
	return nil, status.Error(codes.Unimplemented, "GetHead not implemented yet")
}

func (s *Server) GetTail(ctx context.Context, _ *emptypb.Empty) (*pb.NodeInfo, error) {
	_ = ctx
	return nil, status.Error(codes.Unimplemented, "GetTail not implemented yet")
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
