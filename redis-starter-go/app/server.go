package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type User struct {
	Passwords []string
	Flags     []string
}

type LockableList struct {
	sync.Mutex
	elements []string
}

var ServerMemory sync.Map
var UserRegistry sync.Map
var ListRegistry sync.Map

func main() {

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

		switch {
		case strings.Contains(upperLine, "PING"):
			handlePing(reader, conn, &authUser)

		case strings.Contains(upperLine, "ECHO"):
			handleEcho(reader, conn, &authUser)

		case strings.Contains(upperLine, "SET"):
			handleSet(reader, conn, &authUser)

		case strings.Contains(upperLine, "GET"):
			handleGet(reader, conn, &authUser)

		case strings.Contains(upperLine, "ACL"):
			handleACL(reader, conn, &authUser)

		case strings.Contains(upperLine, "AUTH"):
			handleAuth(reader, conn, &authUser)

		case strings.Contains(upperLine, "RPUSH"):
			handleRPush(reader, conn, &authUser)

		case strings.Contains(upperLine, "LRANGE"):
			handleLRange(reader, conn, &authUser)

		case strings.Contains(upperLine, "LPUSH"):
			handleLPush(reader, conn, &authUser)

		case strings.Contains(upperLine, "LLEN"):
			handleLLen(reader, conn, &authUser)

		case strings.Contains(upperLine, "LPOP"):
			handleLPop(reader, conn, &authUser)

		}
	}
}

func handlePing(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}
	fmt.Print("+PONG\r\n")
	_, err := conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		log.Printf("Write error: %v", err)
		return
	}
}

func handleEcho(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}
	_, _ = reader.ReadString('\n')
	content, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Read error: %v", err)
		return
	}
	content = strings.TrimSpace(content)
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(content), content)
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Printf("Write error: %v", err)
		return
	}
}

func handleSet(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}
	_, _ = reader.ReadString('\n')
	key, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading key: %v", err)
		return
	}
	key = strings.TrimSpace(key)

	_, _ = reader.ReadString('\n')
	value, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading value: %v", err)
		return
	}
	value = strings.TrimSpace(value)
	ServerMemory.Store(key, value)

	_, err = conn.Write([]byte("+OK\r\n"))

	if reader.Buffered() > 0 {
		_, _ = reader.ReadString('\n')
		option, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading the option: %v", err)
			return
		}
		option = strings.ToUpper(strings.TrimSpace(option))

		if option == "PX" {
			_, _ = reader.ReadString('\n')
			tte, err := reader.ReadString('\n')
			tteNum, _ := strconv.Atoi(strings.TrimSpace(tte))
			if err != nil {
				log.Printf("Error reading time to expire: %v", err)
				return
			}

			go func(tte int) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tte)*time.Millisecond)
				defer cancel()
				<-ctx.Done()
				ServerMemory.Delete(key)
			}(tteNum)
		}
	}
}

func handleGet(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}
	_, _ = reader.ReadString('\n')
	key, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading key: %v", err)
		return
	}
	key = strings.TrimSpace(key)
	log.Print(key)
	value, ok := ServerMemory.Load(key)

	if ok {
		stringValue, ok := value.(string)
		if !ok {
			log.Println("The value is not of type string")
			return
		}
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(stringValue), stringValue)
		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}

	_, err = conn.Write([]byte("$-1\r\n"))
	if err != nil {
		log.Printf("Writing Error: %v", err)
		return
	}

}

func handleACL(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}

	_, _ = reader.ReadString('\n')
	cmd, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading the ACL command: %v", err)
		return
	}
	cmd = strings.ToUpper(strings.TrimSpace(cmd))

	switch cmd {
	case "WHOAMI":
		log.Print("user: ", *authUser)
		_, err = conn.Write([]byte("$" + strconv.Itoa(len(*authUser)) + "\r\n" + *authUser + "\r\n"))
		if err != nil {
			log.Print("Writing error: ", err)
			return
		}

	case "GETUSER":
		_, _ = reader.ReadString('\n')
		username, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading the username: %v", err)
		}
		username = strings.TrimSpace(username)
		userInfo, ok := UserRegistry.Load(username)
		if !ok {
			log.Printf("User %s not found", username)
			return
		}
		response := "*4\r\n$5\r\nflags\r\n*" + strconv.Itoa(len(userInfo.(*User).Flags)) + "\r\n"

		if len(userInfo.(*User).Flags) > 0 {
			for _, flag := range userInfo.(*User).Flags {
				response = response + "$" + strconv.Itoa(len(flag)) + "\r\n" + flag + "\r\n"
			}
		}

		response += "$9\r\npasswords\r\n*" + strconv.Itoa(len(userInfo.(*User).Passwords)) + "\r\n"

		if len(userInfo.(*User).Passwords) > 0 {
			for _, password := range userInfo.(*User).Passwords {
				response += "$" + strconv.Itoa(len(password)) + "\r\n" + password + "\r\n"
			}
		}

		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Print("Writing error: ", err)
			return
		}

	case "SETUSER":
		_, _ = reader.ReadString('\n')
		username, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading the username: %v", err)
			return
		}
		username = strings.TrimSpace(username)

		_, _ = reader.ReadString('\n')
		arg, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading the argument: %v", err)
			return
		}
		if strings.HasPrefix(arg, ">") {
			password, _ := strings.CutPrefix(arg, ">")
			password = strings.TrimSpace(password)
			hashBytes := sha256.Sum256([]byte(password))
			hashString := hex.EncodeToString(hashBytes[:])
			val, ok := UserRegistry.Load(username)
			if !ok {
				log.Printf("User %s not found", username)
				return
			}

			userInfo := val.(*User)

			userInfo.Passwords = append(userInfo.Passwords, hashString)
			for i, flag := range userInfo.Flags {
				if flag == "nopass" {
					userInfo.Flags = append(userInfo.Flags[:i], userInfo.Flags[i+1:]...)
					break
				}
			}

		}

		_, err = conn.Write([]byte("+OK\r\n"))
		if err != nil {
			log.Print("Writing error")
		}
	}
}

func handleAuth(reader *bufio.Reader, conn net.Conn, authUser *string) {
	_, _ = reader.ReadString('\n')
	username, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading the username: %v", err)
		return
	}
	username = strings.TrimSpace(username)

	_, _ = reader.ReadString('\n')
	password, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading the password: %v", err)
		return
	}
	password = strings.TrimSpace(password)

	hashedPassBytes := sha256.Sum256([]byte(password))
	hashedPassStr := hex.EncodeToString(hashedPassBytes[:])
	userInfo, ok := UserRegistry.Load(username)
	isPassword := false
	if !ok {
		log.Printf("User %s not found", username)
	}
	for _, pass := range userInfo.(*User).Passwords {
		if pass == hashedPassStr {
			isPassword = true
			break
		}
	}

	if isPassword {
		*authUser = username
		fmt.Printf("Debug: Session user changed to %s\n", *authUser)
		_, err = conn.Write([]byte("+OK\r\n"))
	} else {
		_, err = conn.Write([]byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n"))
	}
}

func handleRPush(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}

	_, _ = reader.ReadString('\n')
	listKey, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading list key: ", err)
		return
	}
	listKey = strings.TrimSpace(listKey)

	var newElems []string
	for reader.Buffered() > 0 {
		_, _ = reader.ReadString('\n')
		elem, err := reader.ReadString('\n')
		if err != nil {
			log.Print("Error reading element: ", err)
			return
		}
		elem = strings.TrimSpace(elem)
		newElems = append(newElems, elem)
	}

	listInterface, _ := ListRegistry.LoadOrStore(listKey, &LockableList{elements: []string{}})

	list := listInterface.(*LockableList)

	list.Lock()
	defer list.Unlock()

	list.elements = append(list.elements, newElems...)
	response := fmt.Sprintf(":%d\r\n", len(list.elements))
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Printf("Writing Error: %v", err)
		return
	}

}

func handleLRange(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}

	var response string

	_, _ = reader.ReadString('\n')
	listKey, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading list key: ", err)
		return
	}
	listKey = strings.TrimSpace(listKey)

	_, _ = reader.ReadString('\n')
	index1Str, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading first index: ", err)
		return
	}
	index1Str = strings.TrimSpace(index1Str)
	index1, _ := strconv.Atoi(index1Str)

	_, _ = reader.ReadString('\n')
	index2Str, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading second index: ", err)
		return
	}
	index2Str = strings.TrimSpace(index2Str)
	index2, _ := strconv.Atoi(index2Str)

	listInterface, ok := ListRegistry.Load(listKey)
	if !ok {
		response = "*0\r\n"
	} else {
		list := listInterface.(*LockableList)
		list.Lock()
		defer list.Unlock()

		listLength := len(list.elements)

		if index1 < 0 {
			index1 = max(0, index1+listLength)
		}

		if index2 < 0 {
			index2 = max(0, index2+listLength)
		}

		index1 = min(index1, listLength-1)
		index2 = min(index2, listLength-1)

		response = fmt.Sprintf("*%d\r\n", index2-index1+1)
		for i := index1; i <= index2; i++ {
			response += "$" + strconv.Itoa(len(list.elements[i])) + "\r\n" + list.elements[i] + "\r\n"
		}
	}
	_, err = conn.Write([]byte(response))
	if err != nil {
		fmt.Print("Error writing: ", err)
		return
	}
}

func handleLPush(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}

	_, _ = reader.ReadString('\n')
	listKey, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading list key: ", err)
		return
	}
	listKey = strings.TrimSpace(listKey)

	var newElems []string
	for reader.Buffered() > 0 {
		_, _ = reader.ReadString('\n')
		elem, err := reader.ReadString('\n')
		if err != nil {
			log.Print("Error reading element: ", err)
			return
		}
		elem = strings.TrimSpace(elem)
		newElems = append(newElems, elem)
	}

	listInterface, _ := ListRegistry.LoadOrStore(listKey, &LockableList{elements: []string{}})

	list := listInterface.(*LockableList)

	list.Lock()
	defer list.Unlock()
	for i := range newElems {
		list.elements = append([]string{newElems[i]}, list.elements...)
	}
	response := fmt.Sprintf(":%d\r\n", len(list.elements))
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Printf("Writing Error: %v", err)
		return
	}
}

func handleLLen(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !checkAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}

	_, _ = reader.ReadString('\n')
	listKey, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading list key: ", err)
		return
	}
	listKey = strings.TrimSpace(listKey)
	var response string
	listInterface, ok := ListRegistry.Load(listKey)
	if !ok {
		response = ":0\r\n"
	} else {
		list := listInterface.(*LockableList)
		list.Lock()
		defer list.Unlock()
		listLength := len(list.elements)
		response = fmt.Sprintf(":%d\r\n", listLength)
	}

	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Print("Writing error: ", err)
	}

}

func handleLPop(reader *bufio.Reader, conn net.Conn, authUser *string) {
	_, _ = reader.ReadString('\n')
	listKey, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading list key: ", err)
		return
	}
	listKey = strings.TrimSpace(listKey)
	arg := 1
	if reader.Buffered() > 0 {
		_, _ = reader.ReadString('\n')
		argStr, err := reader.ReadString('\n')
		if err != nil {
			log.Print("Error reading argument: ", err)
			return
		}
		argStr = strings.TrimPrefix(argStr, "$")
		arg, err = strconv.Atoi(strings.TrimSpace(argStr))
		if err != nil {
			log.Print("Argument is not an integer: ", err)
			return
		}
	}

	var response string
	listInterface, ok := ListRegistry.Load(listKey)
	if !ok {
		response = "$-1\r\n"
		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Print("Writing error: ", err)
			return
		}
		return
	}
	list := listInterface.(*LockableList)
	list.Lock()
	defer list.Unlock()
	var poppedElems []string
	for i := 0; i < arg; i++ {
		poppedElems = append(poppedElems, list.elements[i])
	}

	list.elements = list.elements[arg:]
	log.Print(arg)
	if arg == 1 {
		response = fmt.Sprintf("$%d\r\n%s\r\n", len(poppedElems[0]), poppedElems[0])
	} else {
		response = fmt.Sprintf("*%d\r\n", arg)
		for _, elem := range poppedElems {
			elemResponse := fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
			response += elemResponse
		}
	}
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Print("Writing error: ", err)
		return
	}
}

func checkAuth(authUser *string) bool {
	return *authUser != ""
}
