package main

import "fmt"

func main() {
    value := "Welcome, my friend"

    runes := []rune(value)
    safeSubstring := string(runes[0:7])

    fmt.Println(" RUNE SUBSTRING:", safeSubstring)

    asciiSubString := value[0:7]
    fmt.Println("ASCII SUBSTRING:", asciiSubString)

    fmt.Println(value[0] == "W"[0])
}
