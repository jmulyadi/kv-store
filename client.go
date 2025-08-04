package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/jmulyadi/kv-store/proto"

	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewKVStoreClient(conn)
	fmt.Println("Client Starting...")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.Put(ctx, &pb.PutRequest{Key: "name", Value: "Josh"})
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	resp, err := client.Get(ctx, &pb.GetRequest{Key: "name"})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	log.Printf("Got value: %s, found: %v", resp.Value, resp.Found)
	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: "name"})
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
}
