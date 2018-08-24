package v2 

import (
	"net/rpc"
) 

const HelloServiceName = "ch4rpc.HelloService"

type HelloServiceInterface interface {
	Hello(request string, reply *string) error
}

func RegisterHelloService(svc HelloServiceInterface) error {
	return rpc.RegisterName(HelloServiceName, svc)
}
