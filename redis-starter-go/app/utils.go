package main

import (
	"fmt"
	"log"
	"net"
)

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
