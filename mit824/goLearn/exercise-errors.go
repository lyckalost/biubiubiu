package main

import (
    "fmt"
)

func MySqrt(x float64) float64 {
    if (x == 0) {
        return 0
    }

    z := x / 2
    for i := 0; i < 10; i++ {
        z -= (z*z - x) / (2*z)
    }

    return z
}

type ErrNegativeSqrt float64

func (errNegativeSqrt ErrNegativeSqrt) Error() string {
    return fmt.Sprintf("cannot Sqrt negative number: %v", float64(errNegativeSqrt))
}

func Sqrt(x float64) (float64, error) {
    if (x < 0) {
        return 0, ErrNegativeSqrt(x)
    } else {
        return MySqrt(x), nil
    }
}

func main() {
    fmt.Println(Sqrt(2))
    fmt.Println(Sqrt(-2))
}
