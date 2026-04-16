package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
)

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "0123456789"

func CheckAuth(authUser *string) bool {
	return *authUser != ""
}

func SendBLPOPSuccess(conn net.Conn, listKey, value string) {
	response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(listKey), listKey, len(value), value)
	_, err := conn.Write([]byte(response))
	if err != nil {
		log.Print("Writing error: ", err)
	}
}

func RandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func ConnectToMaster(masterHost, masterPort, replicaPort string) {
	masterConn, err := net.Dial("tcp", masterHost+":"+masterPort)
	if err != nil {
		log.Fatal("Error connecting to master: ", err)
		return
	}

	reader := bufio.NewReader(masterConn)

	_, err = masterConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		log.Print("Error sending PING to master: ", err)
		return
	}

	_, _ = reader.ReadString('\n')

	HandleReplConfSlave(masterConn, reader, replicaPort)
	HandlePsyncSlave(masterConn, reader)
}

func PropagateCommand(args ...string) {
	ReplicasMu.Lock()
	defer ReplicasMu.Unlock()

	if len(Replicas) == 0 {
		return
	}

	resp := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	MasterOffset += len(resp)

	for _, conn := range Replicas {
		_, err := conn.Conn.Write([]byte(resp))
		if err != nil {
			log.Printf("Error propagating to replica %v: %s", conn, err)
		}
	}
}

func OffsetByteSize(args ...string) int {
	size := len(fmt.Sprintf("*%d\r\n", len(args)))

	for _, arg := range args {
		size += len(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}

	return size
}
