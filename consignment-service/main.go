// consignment-service/main.go
package main

import (
	"context"
	"log"
	"net"
	"sync"

	// import generated protobuf code
	pb "microservice_tutorial/consignment-service/proto/consignment"
	grpc "google.golang.org/grpc"
	reflection "google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

type repository interface {
	Create(*pb.Consignment) (*pb.Consignment, error)
	GetAll() []*pb.Consignment
}

// Repository - dummy repository, this simulates the use of a datastore
// of some kin. We'll replace this with a real implementation later on.
type Repository struct {
	mu sync.RWMutex
	consignments []*pb.Consignment
}

// Create a new consignment
func (repo *Repository) Create(consignment *pb.Consignment) (*pb.Consignment, error) {
	repo.mu.Lock()
	updated := append(repo.consignments, consignment)
	repo.consignments = updated
	repo.mu.Unlock()
	return consignment, nil
}

// Get all consignments
func (repo *Repository) GetAll() []*pb.Consignment {
	return repo.consignments
}

// Service should implement all of the methods to satisfy the service
// we defined in out protobuf definition. You can check the interface
// in the generated code itself fo the exact method signatures etc
// to give you a better idea
type service struct {
	repo repository
}

// CreateConsignment - we create just one method on our service,
// which is a create method, which takes a context and a request as an
// argument, these are handled by the grpc server.
func (s *service) CreateConsignment(ctx context.Context, req *pb.Consignment) (*pb.Response, error) {
	log.Println("Creating new consignment")
	// Save our consignment
	consignment, err := s.repo.Create(req)
	if err != nil {
		return nil, err
	}

	// Return matching `Response` message we create in our
	// protobuf definition
	return &pb.Response{Created: true, Consignment: consignment}, nil
}

func (s *service) GetConsignments(ctx context.Context, req *pb.GetRequest) (*pb.Response, error) {
	log.Println("Returning all consignments")
	consignments := s.repo.GetAll()
	return &pb.Response{Consignments: consignments}, nil
}

func main() {
	repo := &Repository{}

	// Setup gRPC server
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	// Register our service with the gPRC server, this will tie our 
	// implementation into the auto-generated interface code for our
	// protobuf definition
	pb.RegisterShippingServiceServer(s, &service{repo})

	// Register reflection service on gRPC server
	reflection.Register(s)

	log.Println("Running on port: ", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}