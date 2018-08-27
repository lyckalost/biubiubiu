package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "ch4rpc/ep5/sub1cert/hello"
	"net"
	"context"
	"log"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

type HelloServiceImpl struct {}

func (p *HelloServiceImpl) Hello (ctx context.Context, req *pb.String) (*pb.String, error) {
	return &pb.String{Value:"safe hello with CA: " + req.GetValue()}, nil
}

func main() {
	conn, err := net.Listen("tcp", ":1234")	
	if err != nil {
		log.Fatal(err)
	}

	certificate, err := tls.LoadX509KeyPair("../server.crt", "../server.key")
	if err != nil {
		log.Fatal(err)
	}

	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile("../ca.crt")
	if err != nil {
		log.Fatal(err)
	}
	if ok := certPool.AppendCertsFromPEM(ca); !ok  {
		log.Fatal("failed to append certs")
	}

	// server怎么验证client的不是特别理解
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: certPool,
	})	

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterHelloServiceServer(grpcServer, new(HelloServiceImpl))
	grpcServer.Serve(conn)
}
