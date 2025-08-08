package main

import (
	"context"
	"flag"
	"log"
	"net"
	"sync"

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
	rf := 3 // Replication factor (configurable)

	// If request is a replica, just store locally and do NOT forward
	if req.GetIsReplica() {
		s.store.Put(req.GetKey(), req.GetValue())
		log.Printf("[Put][Replica] Stored key %q locally on node %s", req.GetKey(), s.cluster.SelfAddress)
		return &pb.PutResponse{Message: "Stored replica successfully"}, nil
	}

	replicas := s.cluster.GetReplicaNodes(req.GetKey(), rf)
	log.Printf("[Put] Key: %s replicating to nodes: %v", req.GetKey(), replicas)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, node := range replicas {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			if n == s.cluster.SelfAddress {
				s.store.Put(req.GetKey(), req.GetValue())
				log.Printf("[Put] Stored key locally on %s", n)
			} else {
				client := s.cluster.GetClient(n)
				replicaReq := &pb.PutRequest{
					Key:       req.GetKey(),
					Value:     req.GetValue(),
					IsReplica: true,
				}
				_, err := client.Put(ctx, replicaReq)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					log.Printf("[Put] Failed to replicate to %s: %v", n, err)
				} else {
					log.Printf("[Put] Replicated key to %s", n)
				}
			}
		}(node)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return &pb.PutResponse{Message: "Replicated successfully"}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	rf := 3 // Replication factor, should match Put

	// If request is a replica, just get locally and do NOT forward
	if req.GetIsReplica() {
		value, found := s.store.Get(req.GetKey())
		if found {
			log.Printf("[Get][Replica] Found key %q locally on node %s", req.GetKey(), s.cluster.SelfAddress)
		} else {
			log.Printf("[Get][Replica] Key %q not found locally on node %s", req.GetKey(), s.cluster.SelfAddress)
		}
		return &pb.GetResponse{Value: value, Found: found}, nil
	}

	replicas := s.cluster.GetReplicaNodes(req.GetKey(), rf)
	log.Printf("[Get] Key: %s querying nodes: %v", req.GetKey(), replicas)

	type result struct {
		value string
		found bool
		err   error
	}

	resultCh := make(chan result, len(replicas))

	for _, node := range replicas {
		go func(n string) {
			if n == s.cluster.SelfAddress {
				value, found := s.store.Get(req.GetKey())
				if found {
					log.Printf("[Get] Found key %q locally on node %s", req.GetKey(), n)
					resultCh <- result{value, true, nil}
				} else {
					log.Printf("[Get] Key %q not found locally on node %s", req.GetKey(), n)
					resultCh <- result{"", false, nil}
				}
			} else {
				client := s.cluster.GetClient(n)
				replicaReq := &pb.GetRequest{
					Key:       req.GetKey(),
					IsReplica: true,
				}
				res, err := client.Get(ctx, replicaReq)
				if err != nil {
					log.Printf("[Get] Error getting key %q from node %s: %v", req.GetKey(), n, err)
					resultCh <- result{"", false, err}
				} else {
					if res.Found {
						log.Printf("[Get] Found key %q on node %s", req.GetKey(), n)
						resultCh <- result{res.Value, true, nil}
					} else {
						log.Printf("[Get] Key %q not found on node %s", req.GetKey(), n)
						resultCh <- result{"", false, nil}
					}
				}
			}
		}(node)
	}

	for i := 0; i < len(replicas); i++ {
		r := <-resultCh
		if r.err == nil && r.found {
			return &pb.GetResponse{Value: r.value, Found: true}, nil
		}
	}

	// If none found or all errored:
	return &pb.GetResponse{Value: "", Found: false}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	rf := 3 // replication factor, should match Put/Get

	// If request is a replica, just delete locally and do NOT forward
	if req.GetIsReplica() {
		s.store.Delete(req.GetKey())
		log.Printf("[Delete][Replica] Key %q deleted locally on node %s", req.GetKey(), s.cluster.SelfAddress)
		return &pb.DeleteResponse{Message: "Deleted replica successfully"}, nil
	}

	replicas := s.cluster.GetReplicaNodes(req.GetKey(), rf)
	log.Printf("[Delete] Key: %s deleting on nodes: %v", req.GetKey(), replicas)

	type delResult struct {
		err error
	}

	resultCh := make(chan delResult, len(replicas))

	for _, node := range replicas {
		go func(n string) {
			if n == s.cluster.SelfAddress {
				s.store.Delete(req.GetKey())
				log.Printf("[Delete] Key %q deleted locally on node %s", req.GetKey(), n)
				resultCh <- delResult{nil}
			} else {
				client := s.cluster.GetClient(n)
				replicaReq := &pb.DeleteRequest{
					Key:       req.GetKey(),
					IsReplica: true,
				}
				_, err := client.Delete(ctx, replicaReq)
				if err != nil {
					log.Printf("[Delete] Error deleting key %q on node %s: %v", req.GetKey(), n, err)
				} else {
					log.Printf("[Delete] Key %q deleted on node %s", req.GetKey(), n)
				}
				resultCh <- delResult{err}
			}
		}(node)
	}

	var failed int
	for i := 0; i < len(replicas); i++ {
		res := <-resultCh
		if res.err != nil {
			failed++
		}
	}

	if failed > 0 {
		return &pb.DeleteResponse{Message: "Delete partially succeeded"}, nil
	}
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
