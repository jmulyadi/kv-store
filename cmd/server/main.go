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
		log.Printf("[Put] Key: %s routed to node: %s (forwarding from %s)", req.GetKey(), target, s.cluster.SelfAddress)
		client := s.cluster.GetClient(target)
		return client.Put(ctx, req)
	}

	log.Printf("[Put] Handling key %s locally on node %s, Value: %s", req.GetKey(), s.cluster.SelfAddress, req.GetValue())
	s.store.Put(req.GetKey(), req.GetValue())
	log.Printf("[Put] Stored key %q successfully on node %s", req.GetKey(), s.cluster.SelfAddress)

	return &pb.PutResponse{Message: "Stored successfully"}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	target := s.cluster.GetResponsibleNode(req.GetKey())

	if target != s.cluster.SelfAddress {
		log.Printf("[Get] Key: %s routed to node: %s (forwarding from %s)", req.GetKey(), target, s.cluster.SelfAddress)
		client := s.cluster.GetClient(target)
		return client.Get(ctx, req)
	}

	log.Printf("[Get] Handling key %s locally on node %s", req.GetKey(), s.cluster.SelfAddress)
	value, found := s.store.Get(req.GetKey())
	if found {
		log.Printf("[Get] Found key %q with value %q on node %s", req.GetKey(), value, s.cluster.SelfAddress)
	} else {
		log.Printf("[Get] Key %q not found on node %s", req.GetKey(), s.cluster.SelfAddress)
	}
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	target := s.cluster.GetResponsibleNode(req.GetKey())

	if target != s.cluster.SelfAddress {
		log.Printf("[Delete] Key: %s routed to node: %s (forwarding from %s)", req.GetKey(), target, s.cluster.SelfAddress)
		client := s.cluster.GetClient(target)
		return client.Delete(ctx, req)
	}

	log.Printf("[Delete] Handling key %s locally on node %s", req.GetKey(), s.cluster.SelfAddress)
	s.store.Delete(req.GetKey())
	log.Printf("[Delete] Deleted key %q successfully on node %s", req.GetKey(), s.cluster.SelfAddress)

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

	hostPort := *selfAddr
	_, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		log.Fatalf("invalid address: %v", err)
	}
	listenAddr := "0.0.0.0:" + port

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, &server{
		store:   store,
		cluster: cm,
	})
	//registers the server
	reflection.Register(grpcServer)

	log.Printf("KV Store gRPC server listening on %s (listening on %s)", *selfAddr, listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
