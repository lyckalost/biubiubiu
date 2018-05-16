package main

import (
    "fmt"
    "time"
    "reflect"
)

type MyError struct {
    When time.Time
    What string
}

func (e *MyError) Error() string {
    fmt.Printf("calling Error!\n")
    return fmt.Sprintf("at %v, %s", e.When, e.What)
}

func run() error {
    return &MyError {
        time.Now(),
        "it didn't work",
    }
}

func main() {
    if err := run(); err != nil {
        fmt.Println(reflect.TypeOf(err))
        fmt.Println(err)
    }
}
