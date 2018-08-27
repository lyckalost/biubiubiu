package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "ch4rpc/ep5/sub1cert/hello"
	"net"
	"context"
	"log"
)

type HelloServiceImpl struct {}

func (p *HelloServiceImpl) Hello (ctx context.Context, req *pb.String) (*pb.String, error) {
	return &pb.String{Value:"safe hello: " + req.GetValue()}, nil
}

func main() {
	conn, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal(err)
	}

	creds, err := credentials.NewServerTLSFromFile("../server.crt", "../server.key")
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterHelloServiceServer(grpcServer, new(HelloServiceImpl))
	grpcServer.Serve(conn)
}


