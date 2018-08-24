package main

import (
	"net/rpc"
	"net"
	"time"
)

type HelloService struct {}

func (h *HelloService) Hello(request string, reply *string) error {
	*reply = "Response from server for MSG:" + request
	return nil
}

func main() {
	rpc.Register(new(HelloService))

	for {
		conn, _ := net.Dial("tcp", "localhost:1234")
		if conn == nil {
			time.Sleep(time.Second)
			continue
		}
		rpc.ServeConn(conn)
		conn.Close()
	}
}
