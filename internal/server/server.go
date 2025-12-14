package server

import (
	pb "github.com/zanostro/razpravljalnica/gen/pb"
	"github.com/zanostro/razpravljalnica/internal/store"
)

type Server struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedControlPlaneServer

	store *store.Store
}

func New(st *store.Store) *Server {
	return &Server{store: st}
}
