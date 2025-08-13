package main

import (
	"context"
	"log"
	"time"

	pb "github.com/jmulyadi/kv/kv-store/proto"
	"google.golang.org/grpc"
)

var nodes = []string{
	"node1:50051",
	"node2:50052",
	"node3:50053",
}

func connectNode(addr string) pb.KVStoreClient {
	var conn *grpc.ClientConn
	var err error

	for i := 0; i < 5; i++ {
		log.Printf("Connecting to node %s", addr)
		conn, err = grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
		if err == nil {
			log.Printf("Connected to %s", addr)
			return pb.NewKVStoreClient(conn)
		}
		log.Printf("Failed to connect to %s, retrying... (%d/5)", addr, i+1)
		time.Sleep(2 * time.Second)
	}

	log.Printf("[ERROR] Could not connect to node %s after retries: %v", addr, err)
	return nil
}

func main() {
	keys := []string{"foo", "bar", "baz"}

	for _, nodeAddr := range nodes {
		client := connectNode(nodeAddr)
		if client == nil {
			continue // skip node if we can't connect
		}

		// Test Put
		for _, key := range keys {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: "value_" + key})
			cancel()
			if err != nil {
				log.Printf("[WARNING] Put failed for key %s on node %s: %v", key, nodeAddr, err)
			} else {
				log.Printf("Put %q successful on node %s", key, nodeAddr)
			}
		}

		// Test Get
		for _, key := range keys {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			res, err := client.Get(ctx, &pb.GetRequest{Key: key})
			cancel()
			if err != nil {
				log.Printf("[WARNING] Get failed for key %s on node %s: %v", key, nodeAddr, err)
			} else if !res.Found {
				log.Printf("Key %q not found on node %s", key, nodeAddr)
			} else {
				log.Printf("Get %q returned: %s on node %s", key, res.Value, nodeAddr)
			}
		}

		// Test Delete
		for _, key := range keys {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
			cancel()
			if err != nil {
				log.Printf("[WARNING] Delete failed for key %s on node %s: %v", key, nodeAddr, err)
			} else {
				log.Printf("Delete %q successful on node %s", key, nodeAddr)
			}

			// Confirm deletion
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			res, err := client.Get(ctx, &pb.GetRequest{Key: key})
			cancel()
			if err != nil {
				log.Printf("[WARNING] Get after delete failed for key %s on node %s: %v", key, nodeAddr, err)
			} else if res.Found {
				log.Printf("[ERROR] Key %q still found after delete on node %s, value: %s", key, nodeAddr, res.Value)
			} else {
				log.Printf("Confirmed %q is deleted on node %s", key, nodeAddr)
			}
		}
	}
}
