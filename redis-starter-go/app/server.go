package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
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
var AofFile *os.File
var AofMu sync.Mutex

func main() {

	fmt.Println("Full OS Args:", os.Args)

	cwd, err := os.Getwd()
	if err != nil {
		log.Print("Warning: could not get the current working directory: ", err)
		cwd = "."
	}

	dir := flag.String("dir", cwd, "The base directory where redis stores its data files")
	dbfilename := flag.String("dbfilename", "rdbfiles", "RDB File")
	port := flag.Int("port", 6379, "The port the server will run on")
	replicaof := flag.String("replicaof", "master", "The server the replica is connected to")
	appendonly := flag.String("appendonly", "no", "Controls whether AOF persistence is enabled or disabled")
	appenddirname := flag.String("appenddirname", "appendonlydir", "The subdirectory under dir where AOF and manifest files are stored")
	appendfilename := flag.String("appendfilename", "appendonly.aof", "The name of the append-only file that records write operations")
	appendfsync := flag.String("appendfsync", "everysec", "How often buffered writes are flushed to the AOF file on disk")

	flag.Parse()

	// log.Println("dir: ", *dir)
	// log.Println("dbfilename: ", *dbfilename)
	// log.Println("port: ", *port)
	// log.Println("replicaof: ", *replicaof)

	EnvVariables.Store("dir", *dir)
	EnvVariables.Store("dbfilename", *dbfilename)
	EnvVariables.Store("appendonly", *appendonly)
	EnvVariables.Store("appenddirname", *appenddirname)
	EnvVariables.Store("appendfilename", *appendfilename)
	EnvVariables.Store("appendfsync", *appendfsync)

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

	if *appendonly == "yes" {
		aofDirPath := filepath.Join(*dir, *appenddirname)

		err = os.MkdirAll(aofDirPath, 0755)
		if err != nil {
			log.Fatal("Failed to create AOF directory: ", err)
		}

		manifestFileName := fmt.Sprintf("%s.manifest", *appendfilename)
		manifestFilePath := filepath.Join(aofDirPath, manifestFileName)

		var activeFileName string

		mainfestData, err := os.ReadFile(manifestFilePath)
		if err == nil {
			lines := strings.Split(string(mainfestData), "\n")
			for _, line := range lines {
				if strings.Contains(line, "type i") {
					parts := strings.Fields(line)
					if len(parts) >= 2 && parts[0] == "file" {
						activeFileName = parts[1]
						break
					}
				}
			}
		}

		if activeFileName == "" {
			activeFileName = fmt.Sprintf("%s.1.incr.aof", *appendfilename)
			manifestContent := fmt.Sprintf("file %s seq 1 type i\n", activeFileName)
			err = os.WriteFile(manifestFilePath, []byte(manifestContent), 0644)
			if err != nil {
				log.Fatal("Failed to write manifest file: ", err)
			}
		}
		aofFilePath := filepath.Join(aofDirPath, activeFileName)

		AOFParser(aofFilePath)

		AofFile, err = os.OpenFile(aofFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("Failed to open AOF file: ", err)
		}

	} else {
		LoadRDB(*dir, *dbfilename)
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

		case "ZCARD":
			HandleZCard(reader, conn, &authUser)

		case "ZSCORE":
			HandleZScore(reader, conn, &authUser)

		case "ZREM":
			HandleZRem(reader, conn, &authUser)

		case "GEOADD":
			HandleGeoAdd(reader, conn, &authUser)

		case "GEOPOS":
			HandleGeoPos(reader, conn, &authUser)

		case "GEODIST":
			HandleGeoDist(reader, conn, &authUser)

		case "GEOSEARCH":
			HandleGeoSearch(reader, conn, &authUser)

		}

	}
}
