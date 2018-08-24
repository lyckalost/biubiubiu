package main

import (
	"google.golang.org/grpc"
	pb "ch4rpc/ep4/sub2stream/stream"
	"context"
	"log"
	"net"
	"io"
)

type HelloServiceImpl struct{}

func (p *HelloServiceImpl) Hello(ctx context.Context, args *pb.String) (*pb.String, error) {
	reply := &pb.String{Value: "hello:" + args.GetValue()}
	return reply, nil
}

func (p *HelloServiceImpl) Channel(stream pb.HelloService_ChannelServer) error {
	for {
		args, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		reply := &pb.String{Value : "hello" + args.GetValue()}

		err = stream.Send(reply)
		if (err != nil) {
			return err
		}
	}
}

func main() {
	grpcServer := grpc.NewServer()
	pb.RegisterHelloServiceServer(grpcServer, new(HelloServiceImpl))

	lis, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal(err)
	}
	grpcServer.Serve(lis)
}
