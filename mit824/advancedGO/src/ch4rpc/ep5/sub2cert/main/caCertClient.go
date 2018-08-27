package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "ch4rpc/ep5/sub1cert/hello"
	"context"
	"log"
	"fmt"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

func main() {
	certificate, err := tls.LoadX509KeyPair("../client.crt", "../client.key")
	if err != nil {
		log.Fatal(err)
	}

	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile("../ca.crt")
	if err != nil {
		log.Fatal(err)
	}
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		log.Fatal("failed to append ca certs")
	}

	tlsServerName := "server.io"
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate},
		ServerName: tlsServerName,
		RootCAs: certPool,
	})

	// client可以理解，从server要证书并且用根证书验证
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
