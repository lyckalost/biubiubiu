package main

import (
	"google.golang.org/grpc"
	pb "ch4rpc/ep4/sub3publish/publish"
	"context"
	"log"
	"io"
	"fmt"
)

func main() {
	conn, err := grpc.Dial("localhost:1234", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewPubsubServiceClient(conn)
	stream, err := client.Subscribe(context.Background(), &pb.String{Value : "golang:"})
	if err != nil {
		log.Fatal(err)
	}

	for {
		reply, err := stream.Recv()
		if err != nil {
			if err == io.EOF{
				break
			}
			log.Fatal(err)
		}
		fmt.Println(reply.GetValue())
	}
}
