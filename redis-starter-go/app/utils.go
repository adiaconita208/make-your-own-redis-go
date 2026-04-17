package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
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

func AddToSortedSet(setKey, elementKey string, score float64) int {
	setInterface, _ := ZSetRegistry.LoadOrStore(setKey, &SortedSet{Elements: make(map[string]float64), Order: make([]SortedSetElement, 0)})
	sortedSet := setInterface.(*SortedSet)

	sortedSet.Lock()
	added := 0
	if _, exists := sortedSet.Elements[elementKey]; !exists {
		added = 1
	} else {
		for i, elem := range sortedSet.Order {
			if elem.Name == elementKey {
				sortedSet.Order = append(sortedSet.Order[:i], sortedSet.Order[i+1:]...)
				break
			}
		}
	}
	sortedSet.Elements[elementKey] = score

	index := sort.Search(len(sortedSet.Order), func(i int) bool {
		if sortedSet.Order[i].Score == score {
			return sortedSet.Order[i].Name >= elementKey
		}

		return sortedSet.Order[i].Score > score
	})

	sortedSet.Order = append(sortedSet.Order, SortedSetElement{})

	copy(sortedSet.Order[index+1:], sortedSet.Order[index:])

	sortedSet.Order[index] = SortedSetElement{Name: elementKey, Score: score}
	sortedSet.Unlock()

	return added
}
