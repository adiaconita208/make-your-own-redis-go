package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var ServerMemory sync.Map
var UserRegistry sync.Map
var ListRegistry sync.Map
var EnvVariables sync.Map
var ServerInfo sync.Map
var Replicas []*Replica
var ReplicasMu sync.Mutex
var MasterOffset int
var ZSetRegistry sync.Map

func main() {

	fmt.Println("Full OS Args:", os.Args)

	dir := flag.String("dir", "/tmp/redis-data", "File path to where RDB file is stored")
	dbfilename := flag.String("dbfilename", "rdbfiles", "RDB File")
	port := flag.Int("port", 6379, "The port the server will run on")
	replicaof := flag.String("replicaof", "master", "The server the replica is connected to")

	flag.Parse()

	log.Println("dir: ", *dir)
	log.Println("dbfilename: ", *dbfilename)
	log.Println("port: ", *port)
	log.Println("replicaof: ", *replicaof)

	EnvVariables.Store("dir", *dir)
	EnvVariables.Store("dbfilename", *dbfilename)

	LoadRDB(*dir, *dbfilename)

	strPort := strconv.Itoa(*port)

	listener, err := net.Listen("tcp", ":"+strPort)
	if err != nil {
		log.Fatal("Error listening: ", err)
	}

	defer listener.Close()

	UserRegistry.Store("default", &User{Flags: []string{"nopass"}})

	if *replicaof != "master" {
		ServerInfo.Store("role", "slave")
		replicaofSlice := strings.Split(*replicaof, " ")
		masterHost := replicaofSlice[0]
		masterPort := replicaofSlice[1]
		ServerInfo.Store("masterHost", masterHost)
		ServerInfo.Store("masterPort", masterPort)
		ConnectToMaster(masterHost, masterPort, strPort)
	} else {
		ServerInfo.Store("role", "master")
		ServerInfo.Store("master_replid", RandomString(40))
		ServerInfo.Store("master_repl_offset", "0")
	}

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
	reader := bufio.NewReader(conn)
	serveCommands(conn, reader)
}

func serveCommands(conn net.Conn, reader *bufio.Reader) {
	var authUser string
	if defUser, _ := UserRegistry.Load("default"); len(defUser.(*User).Passwords) == 0 {
		authUser = "default"
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Error reading main command: %v", err)
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

		case "INFO":
			HandleInfo(reader, conn, &authUser)

		case "REPLCONF":
			HandleReplConf(reader, conn, &authUser)

		case "PSYNC":
			HandlePsyncMaster(reader, conn, &authUser)

		case "WAIT":
			HandleWait(reader, conn, &authUser)

		case "ZADD":
			HandleZadd(reader, conn, &authUser)

		case "ZRANK":
			HandleZRank(reader, conn, &authUser)

		case "ZRANGE":
			HandleZRange(reader, conn, &authUser)
		}

	}
}
