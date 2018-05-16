package main

import "fmt"

func main() {
    const sample = "\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98"

    fmt.Println(sample)

    for i := 0; i < len(sample); i++ {
        fmt.Printf("%c ", sample[i])
    }
    fmt.Println()
    fmt.Println("sample len:%d", len(sample))

    fmt.Println("Printf with %c:")
    fmt.Printf("%c\n", sample)

    fmt.Println("Printf with %x:")
    fmt.Printf("%x\n", sample)

    fmt.Println("Printf with % x:")
    fmt.Printf("% x\n", sample)

    fmt.Println("Printf with %q:")
    fmt.Printf("%q\n", sample)

    fmt.Println("Printf with %+q:")
    fmt.Printf("%+q\n", sample)
}
