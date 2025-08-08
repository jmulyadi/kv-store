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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keys := []string{"foo", "bar", "baz", "qux"}
	for _, key := range keys {
		_, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: "value_" + key})
		if err != nil {
			log.Fatalf("Put failed for key %s: %v", key, err)
		}
		log.Printf("Put %q successful", key)

		res, err := client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			log.Fatalf("Get failed for key %s: %v", key, err)
		}
		if !res.Found {
			log.Printf("Key %q not found", key)
		} else {
			log.Printf("Get %q returned: %s", key, res.Value)
		}
	}
}
