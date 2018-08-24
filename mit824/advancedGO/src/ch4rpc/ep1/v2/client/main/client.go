package main

import (
	"log"
	"net/rpc"
	"ch4rpc/ep1/v2"
	"fmt"
)

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	var reply string
	err = client.Call(v2.HelloServiceName + ".Hello", "hello", &reply)
	fmt.Println(reply)
	if err != nil {
		log.Fatal(err)
	}
}
