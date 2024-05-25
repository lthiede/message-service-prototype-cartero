package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"log"
	"net"

	pb "github.com/lthiede/cartero/proto"
	"google.golang.org/protobuf/proto"
)

var aFlag = flag.String("a", "", "address")
var pFlag = flag.Bool("p", false, "use protobuf")

func main() {
	flag.Parse()
	listener, err := net.Listen("tcp", *aFlag)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		if *pFlag {
			go handleConnProto(conn)
		} else {
			go handleConn(conn)
		}
	}
}

func handleConn(conn net.Conn) {
	for {
		ping := make([]byte, 14)
		n, err := conn.Read(ping)
		if err != nil {
			log.Fatal(err)
		}
		if n != 14 {
			log.Fatal("unexpected message length")
		}
		_, err = conn.Write(ping)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func handleConnProto(conn net.Conn) {
	for {
		reqLengthBytes := make([]byte, 4)
		n, err := conn.Read(reqLengthBytes)
		if err != nil {
			log.Fatalf("failed to read request length: %v", err)
		}
		if n != 4 {
			log.Fatalf("not enough bytes encoding response length: %d", n)
		}
		var reqLength uint32
		err = binary.Read(bytes.NewReader(reqLengthBytes), binary.BigEndian, &reqLength)
		if err != nil {
			log.Fatalf("failed to encode request length: %v", err)
		}
		pingBytes := make([]byte, reqLength)
		n, err = conn.Read(pingBytes)
		if err != nil {
			log.Fatalf("failed to read request: %v", err)
		}
		if n != int(reqLength) {
			log.Fatalf("request is too short: %d", n)
		}
		ping := pb.Ping{}
		err = proto.Unmarshal(pingBytes, &ping)
		if err != nil {
			log.Fatalf("failed to unmarshal ping: %v", err)
		}

		pong := pb.Ping{}
		pongBytes, err := proto.Marshal(&pong)
		if err != nil {
			log.Fatalf("failed to marshal pong: %v", err)
		}
		pongWireMessage := make([]byte, 0, 4+len(pongBytes))
		pongWireMessage = binary.BigEndian.AppendUint32(pongWireMessage, uint32(len(pongBytes)))
		pongWireMessage = append(pongWireMessage, pongBytes...)
		_, err = conn.Write(pongWireMessage)
		if err != nil {
			log.Fatalf("failed to send response: %v", err)
		}
	}
}
