package main

import (
	"net"
	"net/rpc"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type KVStoreService struct {
	m map[string]string
	filter map[string]func(key string)
	mu sync.Mutex
}

func (p *KVStoreService) Get(key string, value *string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if v, ok := p.m[key]; ok {
		*value = v
		return nil
	}
	return nil
}

func (p *KVStoreService) Set(kv [2]string, reply *struct{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	key, value := kv[0], kv[1]

	fmt.Printf("MAP len:%d\n", len(p.filter))
	if oldValue := p.m[key]; oldValue != value {
		for k, fn := range p.filter {
			fmt.Printf("filter %s\n", k)
			fn(key)
		}
	}

	p.m[key] = value
	return nil

}

func (p *KVStoreService) Watch(timeoutSecond int, keyChanged *string) error {
	id := fmt.Sprintf("watch-%s-%03d", time.Now(), rand.Int())
	fmt.Printf("Watch ID: %s\n", id)
	ch := make(chan string, 10) //buffered

	p.mu.Lock()
	fmt.Printf("Assigning filter\n")
	p.filter[id] = func(key string) {ch <- key}
	p.mu.Unlock()

	fmt.Printf("Assigning filter Done!\n")

	select {
	case <- time.After(time.Duration(timeoutSecond) * time.Second):
		return fmt.Errorf("timeout")
	case key := <-ch:
		*keyChanged = key
		fmt.Printf("KeyChanged:%s\n", *keyChanged)
		return nil
	}
	return nil
}

func NewKVStoreService() *KVStoreService {
	return &KVStoreService {
		m : make(map[string]string),
		filter : make(map[string]func(key string)),
	}
}

func main() {
	rpc.RegisterName("KVStoreService", NewKVStoreService())
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("Listen TCP error:", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error", err)
		}
		go rpc.ServeConn(conn)
	}
}
