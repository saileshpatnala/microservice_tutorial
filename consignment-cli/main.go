// consignment-cli/main.go
package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"context"

	pb "microservice_tutorial/consignment-service/proto/consignment"
	grpc "google.golang.org/grpc"
)

const (
	address = "localhost:50051"
	defaultFileName = "consignment.json"
)

func parseFile(file string) (*pb.Consignment, error) {
	var consignment *pb.Consignment
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data, &consignment)
	return consignment, err
}

func main() {
	// Set up connection to the server
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewShippingServiceClient(conn)

	// Contact the server and print out its response
	file := defaultFileName
	if len(os.Args) > 1 {
		file = os.Args[1]
	}

	consignment, err := parseFile(file)
	if err != nil {
		log.Fatalf("Could not parse file: %v", err)
	}

	r, err := client.CreateConsignment(context.Background(), consignment)
	if err != nil {
		log.Fatalf("Could not greet: %v", err)
	}

	log.Printf("Create: %t", r.Created)

	getAll, err := client.GetConsignments(context.Background(), &pb.GetRequest{})
	if err != nil {
		log.Fatalf("Could not list consignments: %v", err)
	}
	for _, v := range getAll.Consignments {
		log.Println(v)
	}
}