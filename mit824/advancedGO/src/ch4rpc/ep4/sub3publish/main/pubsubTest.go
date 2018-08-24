package main

import (
	"github.com/docker/docker/pkg/pubsub"
	"time"
	"fmt"
	"strings"
)

func main() {
	p := pubsub.NewPublisher(100*time.Millisecond, 10)	

	// type assertions
	// https://golang.org/ref/spec#Type_assertions
	golang := p.SubscribeTopic(func(v interface{}) bool {
		if key, ok := v.(string); ok {
			if strings.HasPrefix(key, "golang:") {
				return true
			}
		}
		return false
	})

	docker := p.SubscribeTopic(func(v interface{}) bool {
		if key, ok := v.(string); ok {
			if strings.HasPrefix(key, "docker:") {
				return true
			}
		}
		return false
	})

	numbers := p.SubscribeTopic(func(v interface{}) bool {
		if _, ok := v.(int); ok {
			return true
		}
		return false
	})

	go p.Publish(666)
	go p.Publish("hi")
	go p.Publish("golang: https://golang.org")
	go p.Publish("docker: https://www.docker.com")
	time.Sleep(1)

	go func() {
		fmt.Println("numbers:", <-numbers)
	} ()

	go func() {
		fmt.Println("golang topic:", <-golang)
	} ()

	go func() {
		fmt.Println("docker topic", <-docker)
	} ()

	time.Sleep(5 * time.Second)
}
