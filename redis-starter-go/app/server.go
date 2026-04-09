package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

type User struct {
	Passwords []string
	Flags     []string
}

type LockableList struct {
	sync.Mutex
	elements []string
	clients  []chan string
}

var ServerMemory sync.Map
var UserRegistry sync.Map
var ListRegistry sync.Map
var EnvVariables sync.Map

func main() {

	fmt.Println("Full OS Args:", os.Args)

	dir := flag.String("dir", "/tmp/redis-data", "File path to where RDB file is stored")
	dbfilename := flag.String("dbfilename", "rdbfiles", "RDB File")

	flag.Parse()

	log.Println("dir: ", *dir)
	log.Println("dbfilename: ", *dbfilename)

	EnvVariables.Store("dir", *dir)
	EnvVariables.Store("dbfilename", *dbfilename)

	LoadRDB(*dir, *dbfilename)

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal("Error listening: ", err)
	}

	defer listener.Close()

	UserRegistry.Store("default", &User{Flags: []string{"nopass"}})

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	var authUser string
	if defUser, _ := UserRegistry.Load("default"); len(defUser.(*User).Passwords) == 0 {
		authUser = "default"
	}
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Read error: %v", err)
			continue
		}

		upperLine := strings.ToUpper(strings.TrimSpace(line))

		switch upperLine {
		case "PING":
			HandlePing(reader, conn, &authUser)

		case "ECHO":
			HandleEcho(reader, conn, &authUser)

		case "SET":
			HandleSet(reader, conn, &authUser)

		case "GET":
			HandleGet(reader, conn, &authUser)

		case "ACL":
			HandleACL(reader, conn, &authUser)

		case "AUTH":
			HandleAuth(reader, conn, &authUser)

		case "RPUSH":
			HandleRPush(reader, conn, &authUser)

		case "LRANGE":
			HandleLRange(reader, conn, &authUser)

		case "LPUSH":
			HandleLPush(reader, conn, &authUser)

		case "LLEN":
			HandleLLen(reader, conn, &authUser)

		case "LPOP":
			HandleLPop(reader, conn, &authUser)

		case "BLPOP":
			HandleBLPop(reader, conn, &authUser)

		case "CONFIG":
			HandleConfigGet(reader, conn, &authUser)

		case "KEYS":
			HandleKeys(reader, conn, &authUser)
		}
	}
}
