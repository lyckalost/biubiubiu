package main

import (
	"log"
	"fmt"
	"net/rpc"
	"time"
)

func doClientWork(client *rpc.Client) {
	go func() {
		var keyChanged string
		err := client.Call("KVStoreService.Watch", 30, &keyChanged)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("watch:", keyChanged)
	} ()
	// This sleep is needed, otherwise set() is called before watch() somehow, there is no watch id in server
	time.Sleep(time.Second * 3)

	fmt.Printf("Setting KEY:%s, VALUE:%s\n", "abc", "abc-value")
	err := client.Call("KVStoreService.Set", [2]string{"abc", "abc-value"}, new(struct{}))
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second * 3)
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	doClientWork(client)
}
