package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "ch4rpc/ep5/sub1cert/hello"
	"context"
	"log"
	"fmt"
)

func main() {
	creds, err := credentials.NewClientTLSFromFile("../server.crt", "server.grpc.io")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.Dial("localhost:1234", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewHelloServiceClient(conn)
	reply, err := client.Hello(context.Background(), &pb.String{Value : "hello"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply.GetValue())
}
