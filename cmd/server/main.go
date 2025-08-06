package main

import (
	"context"
	"flag"
	"log"
	"net"

	"github.com/jmulyadi/kv-store/internal/cluster"
	"github.com/jmulyadi/kv-store/internal/kvstore"
	pb "github.com/jmulyadi/kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedKVStoreServer
	store   *kvstore.KVStore
	cluster *cluster.ClusterManager
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	target := s.cluster.GetResponsibleNode(req.GetKey())
	if target != s.cluster.SelfAddress {
		client := s.cluster.GetClient(target)
		//forwarding to correct node if it's not this node
		return client.Put(ctx, req)
	}
	log.Printf("[Put] Key: %s, Value: %s", req.GetKey(), req.GetValue())
	s.store.Put(req.GetKey(), req.GetValue())
	log.Printf("[Put] Stored key %q successfully", req.GetKey())
	return &pb.PutResponse{Message: "Stored successfully"}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {

	target := s.cluster.GetResponsibleNode(req.GetKey())
	if target != s.cluster.SelfAddress {
		client := s.cluster.GetClient(target)
		//forwarding to correct node if it's not this node
		return client.Get(ctx, req)
	}
	log.Printf("[Get] Key: %s", req.GetKey())
	value, found := s.store.Get(req.GetKey())
	if found {
		log.Printf("[Get] Found key %q with value %q", req.GetKey(), value)
	} else {
		log.Printf("[Get] Key %q not found", req.GetKey())
	}
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	target := s.cluster.GetResponsibleNode(req.GetKey())
	if target != s.cluster.SelfAddress {
		client := s.cluster.GetClient(target)
		//forwarding to correct node if it's not this node
		return client.Delete(ctx, req)
	}
	log.Printf("[Delete] Key: %s", req.GetKey())
	s.store.Delete(req.GetKey())
	log.Printf("[Delete] Deleted key %q successfully", req.GetKey())
	return &pb.DeleteResponse{Message: "Deleted successfully"}, nil
}

var selfAddr = flag.String("addr", "localhost:50051", "Address to listen on")

func main() {
	flag.Parse()
	//use three nodes
	allNodes := []string{
		"node1:50051",
		"node2:50052",
		"node3:50053",
	}

	cm := cluster.NewClusterManager(*selfAddr, allNodes, 100)
	store := kvstore.NewKVStore()

	lis, err := net.Listen("tcp", *selfAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, &server{
		store:   store,
		cluster: cm,
	})

	reflection.Register(grpcServer)

	log.Printf("KV Store gRPC server listening on %s", *selfAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
