package cluster

import (
	"sync"

	pb "github.com/jmulyadi/kv/kv-store/proto"
	"google.golang.org/grpc"
)

type ClusterManager struct {
	SelfAddress string
	Nodes       []string
	clients     map[string]pb.KVStoreClient
	mu          sync.Mutex
}

func NewClusterManager(self string, allNodes []string) *ClusterManager {
	return &ClusterManager{
		SelfAddress: self,
		Nodes:       allNodes,
		clients:     make(map[string]pb.KVStoreClient),
	}
}

func (c *ClusterManager) GetReplicaNodes(key string, rf int) []string {
	// naive round-robin/fixed selection for now
	if rf > len(c.Nodes) {
		rf = len(c.Nodes)
	}
	return c.Nodes[:rf]
}

func (c *ClusterManager) GetClient(node string) pb.KVStoreClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.clients[node]; ok {
		return client
	}
	conn, _ := grpc.Dial(node, grpc.WithInsecure())
	client := pb.NewKVStoreClient(conn)
	c.clients[node] = client
	return client
}
