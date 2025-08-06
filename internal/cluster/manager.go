package cluster

import (
	"log"
	"sync"

	"github.com/jmulyadi/kv-store/internal/hash"
	pb "github.com/jmulyadi/kv-store/proto"
	"google.golang.org/grpc"
)

type ClusterManager struct {
	Nodes       []string
	SelfAddress string
	HashRing    *hash.HashRing
	clients     map[string]pb.KVStoreClient // nodeAddr -> gRPC client
	mu          sync.RWMutex
}

func NewClusterManager(selfAddr string, allNodes []string, replicas int) *ClusterManager {
	ring := hash.New(replicas)
	ring.Add(allNodes...)

	cm := &ClusterManager{
		SelfAddress: selfAddr,
		Nodes:       allNodes,
		HashRing:    ring,
		clients:     make(map[string]pb.KVStoreClient),
	}

	for _, node := range allNodes {
		if node == selfAddr {
			continue // skip self
		}
		conn, err := grpc.Dial(node, grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to connect to node %s: %v", node, err)
			continue
		}
		client := pb.NewKVStoreClient(conn)
		cm.clients[node] = client
	}

	return cm
}

// Returns whether to handle the request locally or forward it
func (cm *ClusterManager) GetResponsibleNode(key string) string {
	return cm.HashRing.Get(key)
}

func (cm *ClusterManager) GetClient(addr string) pb.KVStoreClient {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.clients[addr]
}
