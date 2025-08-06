package kvstore_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/jmulyadi/kv-store/internal/kvstore"
	pb "github.com/jmulyadi/kv-store/proto"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type testServer struct {
	pb.UnimplementedKVStoreServer
	store *kvstore.KVStore
}

func (s *testServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.store.Put(req.GetKey(), req.GetValue())
	return &pb.PutResponse{Message: "Stored successfully"}, nil
}

func (s *testServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, found := s.store.Get(req.GetKey())
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func (s *testServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.store.Delete(req.GetKey())
	return &pb.DeleteResponse{Message: "Deleted successfully"}, nil
}

func startTestGRPCServer(t *testing.T) (pb.KVStoreClient, func()) {
	// Start test server
	lis, err := net.Listen("tcp", ":0") // use any available port
	assert.NoError(t, err)

	server := grpc.NewServer()
	store := kvstore.NewKVStore()
	pb.RegisterKVStoreServer(server, &testServer{store: store})

	go func() {
		if err := server.Serve(lis); err != nil {
			log.Fatalf("Test gRPC server failed: %v", err)
		}
	}()

	// Connect client
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Second))
	assert.NoError(t, err)

	client := pb.NewKVStoreClient(conn)

	// Return cleanup
	cleanup := func() {
		server.Stop()
		conn.Close()
	}

	return client, cleanup
}

func TestPutGetDelete(t *testing.T) {
	client, cleanup := startTestGRPCServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Test Put
	_, err := client.Put(ctx, &pb.PutRequest{Key: "name", Value: "Josh"})
	assert.NoError(t, err)

	// Test Get
	resp, err := client.Get(ctx, &pb.GetRequest{Key: "name"})
	assert.NoError(t, err)
	assert.True(t, resp.Found)
	assert.Equal(t, "Josh", resp.Value)

	// Test Delete
	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: "name"})
	assert.NoError(t, err)

	// Test Get again (should be not found)
	resp, err = client.Get(ctx, &pb.GetRequest{Key: "name"})
	assert.NoError(t, err)
	assert.False(t, resp.Found)
}

func TestPutOverwrite(t *testing.T) {
	client, cleanup := startTestGRPCServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Put initial value
	_, err := client.Put(ctx, &pb.PutRequest{Key: "foo", Value: "bar"})
	assert.NoError(t, err)

	// Overwrite value
	_, err = client.Put(ctx, &pb.PutRequest{Key: "foo", Value: "baz"})
	assert.NoError(t, err)

	// Get and verify new value
	resp, err := client.Get(ctx, &pb.GetRequest{Key: "foo"})
	assert.NoError(t, err)
	assert.True(t, resp.Found)
	assert.Equal(t, "baz", resp.Value)
}
