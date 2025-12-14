package server

import (
	pb "github.com/zanostro/razpravljalnica/gen/pb"
	"github.com/zanostro/razpravljalnica/internal/store"
	"github.com/zanostro/razpravljalnica/internal/sub"
)

type Server struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedControlPlaneServer

	store *store.Store
	sub   *sub.Manager
}

func New(st *store.Store) *Server {
	return &Server{
		store: st,
		sub:   sub.NewManager(),
	}
}
