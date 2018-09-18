package main

import (
	http "net/http"
	"log"
	"time"
)

/* 这段代码太精髓了，无敌
   定义了函数类型 封装旧函数，提供一个统一新接口调用旧函数，厉害！
   https://github.com/chai2010/advanced-go-programming-book/blob/master/ch5-web/ch5-03-middleware.md

type Handler interface {
    ServeHTTP(ResponseWriter, *Request)
}

type HandlerFunc func(ResponseWriter, *Request)

func (f HandlerFunc) ServeHTTP(w ResponseWriter, r *Request)
    f(w, r)
}
*/

func hello(wr http.ResponseWriter, r *http.Request) {
	wr.Write([]byte("Hello"))
}

func timeMiddleware(next http.Handler) http.Handler {
	// http.Handler 可以认为是 一个ResponseWriter 和 *Request 的函数类型, 这里将一个旧的handler生成成了新的handler，都还是handler类型
	convertedHandler := http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
		timeStart := time.Now()

		next.ServeHTTP(wr, r)

		timeElapsed := time.Since(timeStart)
		log.Println(timeElapsed)
	})
	return convertedHandler
}

func main() {
	// http.HandlerFunc() 负责讲一个函数转成了http.Handler
	http.Handle("/", timeMiddleware(http.HandlerFunc(hello)))
	err := http.ListenAndServe(":5050", nil)
	if err != nil {
		log.Fatal(err)
	}
}
