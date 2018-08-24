package main

import (
	"fmt"
	"log"
	"ch4rpc/ep1/v3"
)

func main() {
	client, err := v3.DialHelloService("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply string
	err = client.Hello("hello", &reply)
	fmt.Println(reply)
	if err != nil {
		log.Fatal(err)
	}
}
