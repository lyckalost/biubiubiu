package main

import (
	"net/rpc"
	"fmt"
	"log"
)

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("TCPconn Error")
	}

	var loginReply string		
	var helloReply string

	err = client.Call("HelloService.Login", "user1:password", &loginReply)
	if err != nil {
		log.Fatal("Login Failure", err)
		return
	} 

	err = client.Call("HelloService.Hello", "Hi", &helloReply)
	if err != nil {
		log.Fatal("Hello Failure", err)
	}
	fmt.Println(helloReply)
}
