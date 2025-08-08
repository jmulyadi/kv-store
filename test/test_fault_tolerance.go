package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "github.com/jmulyadi/kv-store/proto"
	"google.golang.org/grpc"
)

var nodeAddr = flag.String("addr", "localhost:50051", "Node address to connect to")

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*nodeAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVStoreClient(conn)

	// Keys to test
	keys := []string{"foo", "bar", "baz"}

	// Test Put
	for _, key := range keys {
		val := "value_" + key
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: val})
		cancel()
		if err != nil {
			log.Fatalf("Put failed for key %s: %v", key, err)
		}
		log.Printf("Put %q successful", key)
	}

	// Test Get
	for _, key := range keys {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := client.Get(ctx, &pb.GetRequest{Key: key})
		cancel()
		if err != nil {
			log.Fatalf("Get failed for key %s: %v", key, err)
		}
		if !res.Found {
			log.Printf("Key %q not found", key)
		} else {
			log.Printf("Get %q returned: %s", key, res.Value)
		}
	}

	// Test Delete
	for _, key := range keys {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
		cancel()
		if err != nil {
			log.Fatalf("Delete failed for key %s: %v", key, err)
		}
		log.Printf("Delete %q successful", key)

		// Confirm deletion by Get
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		res, err := client.Get(ctx, &pb.GetRequest{Key: key})
		cancel()
		if err != nil {
			log.Fatalf("Get after delete failed for key %s: %v", key, err)
		}
		if res.Found {
			log.Printf("ERROR: Key %q still found after delete, value: %s", key, res.Value)
		} else {
			log.Printf("Confirmed %q is deleted", key)
		}
	}
}
