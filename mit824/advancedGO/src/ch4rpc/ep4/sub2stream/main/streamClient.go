package main

import (
	"google.golang.org/grpc"
	pb "ch4rpc/ep4/sub2stream/stream"
	"context"
	"log"
	"io"
	"time"
	"fmt"
)

func main () {
	conn, err := grpc.Dial("localhost:1234", grpc.WithInsecure())
	if (err != nil) {
		log.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewHelloServiceClient(conn)

	stream, err := client.Channel(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			if err := stream.Send(&pb.String{Value:"hi"}); err != nil {
				log.Fatal(err)
			}
			time.Sleep(time.Second)
		}
	} ()

	for {
		reply, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		fmt.Println(reply.GetValue())
	}
}


