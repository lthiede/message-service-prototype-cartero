package main

// #cgo LDFLAGS: -L${SRCDIR}/AnyBlob/build/Release -lAnyBlob -luring -lssl -lcrypto
// #cgo CPPFLAGS: -I${SRCDIR}/AnyBlob/include -std=c++20
// #include "anyblob_wrapper.h"
import "C"
import "fmt"

func main() {
	fmt.Println("Hello from GO")
	i := C.hello()
	fmt.Printf("Returned by C %d \n", i)
}
