package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/jmulyadi/kv-store/proto"

	"google.golang.org/grpc"

	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedKVStoreServer
	store sync.Map
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.store.Store(req.GetKey(), req.GetValue())
	return &pb.PutResponse{Message: "Stored successfully"}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if value, ok := s.store.Load(req.GetKey()); ok {
		return &pb.GetResponse{Value: value.(string), Found: true}, nil
	}
	return &pb.GetResponse{Found: false}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.store.Delete(req.GetKey())
	return &pb.DeleteResponse{Message: "Deleted successfully"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, &server{})

	reflection.Register(grpcServer)

	fmt.Println("KV Store gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
