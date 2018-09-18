package main

import (
	"net/http"
	"log"	
	"time"
)

type middleware func(http.Handler) http.Handler

type Router struct {
	// 我靠，这里原文打个括号会死么...
	middlewareChain [](func(http.Handler) http.Handler)
	mux map[string]http.Handler
}

func NewRouter() *Router {
	return &Router{
		middlewareChain : [](func(http.Handler) http.Handler){},
		mux : map[string]http.Handler{},
	}
}

func (r *Router) Use(m middleware) {
	r.middlewareChain = append(r.middlewareChain, m)
}

func (r *Router) Add(route string, h http.Handler) {
	mergedHandler := h
	for i := len(r.middlewareChain) - 1; i >= 0; i-- {
		mergedHandler = r.middlewareChain[i](mergedHandler)
	}

	r.mux[route] = mergedHandler
}

func (r *Router) Run() error {
	for route, handler := range r.mux {
		http.Handle(route, handler)
	}
	err := http.ListenAndServe(":5050", nil)
	return err
}

func hello(wr http.ResponseWriter, r *http.Request) {
	wr.Write([]byte("hello"))
}

func timeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
		timeStart := time.Now()

		next.ServeHTTP(wr, r)

		timeElapsed := time.Since(timeStart)
		log.Println(timeElapsed)
	})
}

func rateMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(wr, r)
		log.Println("This is rate middleware")
	})
}

func loggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(wr, r)
		log.Println("This is logger middleware")
	})
}

func main() {
	r := NewRouter()
	r.Use(loggerMiddleware)
	r.Use(timeMiddleware)
	r.Use(rateMiddleware)
	r.Add("/", http.HandlerFunc(hello))
	err := r.Run()
	if err != nil {
		log.Fatal(err)
	}
}
