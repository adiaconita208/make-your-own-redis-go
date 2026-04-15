package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func HandlePing(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

	if role, _ := ServerInfo.Load("role"); role == "slave" {
		offsetInterface, _ := ServerInfo.Load("repl_offset")
		offset := offsetInterface.(int)

		ServerInfo.Store("repl_offset", offset+OffsetByteSize("PING"))
	}
}

func HandleEcho(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

func HandleSet(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

	if role, _ := ServerInfo.Load("role"); role == "slave" {
		offsetInterface, _ := ServerInfo.Load("repl_offset")
		offset := offsetInterface.(int)

		size := OffsetByteSize("SET", key, value)
		ServerInfo.Store("repl_offset", offset+size)
	}

	if role, _ := ServerInfo.Load("role"); role == "master" {
		PropagateCommand("SET", key, value)
	}

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

func HandleGet(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

func HandleACL(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

func HandleAuth(reader *bufio.Reader, conn net.Conn, authUser *string) {
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

func HandleRPush(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

	listInterface, _ := ListRegistry.LoadOrStore(listKey, &LockableList{elements: []string{}, clients: []chan string{}})

	list := listInterface.(*LockableList)

	list.Lock()

	responseLen := len(list.elements) + len(newElems)

	for len(list.clients) > 0 && len(newElems) > 0 {

		c := list.clients[0]
		list.clients = list.clients[1:]
		c <- newElems[0]

		newElems = newElems[1:]

	}
	if len(newElems) > 0 {
		list.elements = append(list.elements, newElems...)
	}

	list.Unlock()
	response := fmt.Sprintf(":%d\r\n", responseLen)
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Printf("Writing Error: %v", err)
		return
	}

}

func HandleLRange(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

func HandleLPush(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

func HandleLLen(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

func HandleLPop(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
			return
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

func HandleBLPop(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
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

	_, _ = reader.ReadString('\n')
	timeoutStr, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading timeout: ", err)
		return
	}
	timeout, _ := strconv.ParseFloat(strings.TrimSpace(timeoutStr), 64)

	listInterface, _ := ListRegistry.LoadOrStore(listKey, &LockableList{elements: []string{}, clients: []chan string{}})
	list := listInterface.(*LockableList)
	list.Lock()
	if len(list.elements) > 0 {
		val := list.elements[0]
		list.elements = list.elements[1:]
		list.Unlock()
		SendBLPOPSuccess(conn, listKey, val)
		return
	}

	clientCh := make(chan string, 1)
	list.clients = append(list.clients, clientCh)
	list.Unlock()

	if timeout == 0 {
		val := <-clientCh
		SendBLPOPSuccess(conn, listKey, val)
		return
	}

	timeoutDuration := time.Duration(timeout * float64(time.Second))

	select {
	case val := <-clientCh:
		SendBLPOPSuccess(conn, listKey, val)

	case <-time.After(timeoutDuration):
		list.Lock()

		for i, ch := range list.clients {
			if ch == clientCh {
				list.clients = append(list.clients[:i], list.clients[i+1:]...)
				break
			}
		}

		list.Unlock()
		_, _ = conn.Write([]byte("*-1\r\n"))
	}

}

func HandleConfigGet(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
		}
		return
	}
	_, _ = reader.ReadString('\n')
	_, _ = reader.ReadString('\n')
	_, _ = reader.ReadString('\n')
	keyName, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading value name: ", err)
		return
	}
	keyName = strings.TrimSpace(keyName)

	valueInterface, ok := EnvVariables.Load(keyName)
	if !ok {
		log.Print("Variable not found: ", err)
		return
	}
	value := valueInterface.(string)
	log.Print(value)

	response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(keyName), keyName, len(value), value)

	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Print("Writing error: ", err)
		return
	}

}

func HandleKeys(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
			return
		}
		return
	}

	_, _ = reader.ReadString('\n')
	pattern, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading the pattern", err)
	}
	pattern = strings.TrimSpace(pattern)

	if pattern != "*" {
		_, _ = conn.Write([]byte("-ERR only * pattern is supported\r\n"))
		return
	}

	var keys []string

	ServerMemory.Range(func(key, value any) bool {
		keys = append(keys, key.(string))
		return true
	})

	response := fmt.Sprintf("*%d\r\n", len(keys))
	for _, key := range keys {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
	}

	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Print("Error writing: ", err)
		return
	}
}

func HandleInfo(reader *bufio.Reader, conn net.Conn, authUser *string) {
	if !CheckAuth(authUser) {
		_, err := conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
		if err != nil {
			log.Printf("Writing Error: %v", err)
			return
		}
		return
	}

	_, _ = reader.ReadString('\n')
	arg, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading command argument: ", err)
		return
	}
	arg = strings.ToUpper(strings.TrimSpace(arg))

	switch arg {
	case "REPLICATION":
		serverRoleInterface, _ := ServerInfo.Load("role")
		serverRole := serverRoleInterface.(string)
		roleLine := "role:" + serverRole

		var response string

		if serverRole == "master" {
			replidInterface, _ := ServerInfo.Load("master_replid")
			offsetInterface, _ := ServerInfo.Load("master_repl_offset")

			replid := replidInterface.(string)
			offset := offsetInterface.(string)

			replidLine := "master_replid:" + replid
			offsetLine := "master_repl_offset:" + offset

			content := fmt.Sprintf("%s\n%s\n%s\n", roleLine, replidLine, offsetLine)
			response = fmt.Sprintf("$%d\r\n%s\r\n", len(content), content)
		} else {
			response = fmt.Sprintf("$%d\r\n%s\r\n", len(roleLine), roleLine)
		}

		_, err := conn.Write([]byte(response))
		if err != nil {
			log.Print("Writing error: ", err)
		}
	}
}

func HandleReplConfSlave(conn net.Conn, reader *bufio.Reader, replicaPort string) {
	portCmd := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(replicaPort), replicaPort)
	_, err := conn.Write([]byte(portCmd))
	if err != nil {
		log.Print("Error writing to master", err)
		return
	}

	_, _ = reader.ReadString('\n')

	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		log.Print("Error writing to master", err)
		return
	}

	_, _ = reader.ReadString('\n')
}

// func HandleReplConfMaster(reader *bufio.Reader, conn net.Conn, authUser *string) {
// 	serverRoleInterface, _ := ServerInfo.Load("role")
// 	serverRole := serverRoleInterface.(string)
// 	if serverRole != "master" {
// 		return
// 	}
// 	_, _ = reader.ReadString('\n')
// 	command, err := reader.ReadString('\n')
// 	if err != nil {
// 		log.Print("Error reading REPLCONF subcommand: ", err)
// 		return
// 	}
// 	command = strings.TrimSpace(command)
// 	_, _ = reader.ReadString('\n')
// 	_, _ = reader.ReadString('\n')

// 	if command != "listening-port" && command != "capa" {
// 		log.Print("Unrecognized command: ", command)
// 		return
// 	}
// 	_, err = conn.Write([]byte("+OK\r\n"))
// 	if err != nil {
// 		log.Print("Error writing to slave: ", err)
// 		return
// 	}
// }

func HandleReplConf(reader *bufio.Reader, conn net.Conn, authUser *string) {
	_, _ = reader.ReadString('\n')
	command, err := reader.ReadString('\n')
	if err != nil {
		log.Print("Error reading the REPLCONF subcommand", err)
	}

	command = strings.ToUpper(strings.TrimSpace(command))

	_, _ = reader.ReadString('\n')
	_, _ = reader.ReadString('\n')

	// Handler for the slave server
	if command == "GETACK" {

		offsetInterface, _ := ServerInfo.Load("repl_offset")
		offset := offsetInterface.(int)
		offsetStr := strconv.Itoa(offset)

		response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offsetStr), offsetStr)
		if silentConn, ok := conn.(SilentConn); ok {
			_, _ = silentConn.Conn.Write([]byte(response))
		} else {
			_, _ = conn.Write([]byte(response))
		}

		ServerInfo.Store("repl_offset", offset+37)
		return
	}

	// Handler for the master server
	roleInterface, _ := ServerInfo.Load("role")
	role := roleInterface.(string)
	if role == "master" {
		if command == "LISTENING-PORT" || command == "CAPA" {
			_, err := conn.Write([]byte("+OK\r\n"))
			if err != nil {
				log.Print("Error writing to slave: ", err)
				return
			}
		} else {
			log.Print("Unrecognized subcommand: ", command)
		}
	}
}

func HandlePsyncSlave(conn net.Conn, reader *bufio.Reader) {
	_, err := conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))

	if err != nil {
		log.Print("Error writing to master server: ", err)
		return
	}

	_, _ = reader.ReadString('\n')

	lengthLine, _ := reader.ReadString('\n')
	lengthLine = strings.TrimSpace(strings.TrimPrefix(lengthLine, "$"))
	length, _ := strconv.Atoi(lengthLine)

	rdbBytes := make([]byte, length)
	_, _ = io.ReadFull(reader, rdbBytes)

	ServerInfo.Store("repl_offset", 0)

	go serveCommands(SilentConn{conn}, reader)
}

func HandlePsyncMaster(reader *bufio.Reader, conn net.Conn, authUser *string) {
	serverRoleInterface, _ := ServerInfo.Load("role")
	serverRole := serverRoleInterface.(string)
	if serverRole != "master" {
		return
	}

	_, _ = reader.ReadString('\n')
	_, _ = reader.ReadString('\n')
	_, _ = reader.ReadString('\n')
	_, _ = reader.ReadString('\n')

	replicationIdInterface, _ := ServerInfo.Load("master_replid")
	replicationId := replicationIdInterface.(string)

	replicationOffsetInterface, _ := ServerInfo.Load("master_repl_offset")
	replicationOffset := replicationOffsetInterface.(string)

	response := fmt.Sprintf("+FULLRESYNC %s %s\r\n", replicationId, replicationOffset)

	_, err := conn.Write([]byte(response))
	if err != nil {
		log.Print("Error writing FULLRESYNC: ", err)
		return
	}

	emptyRdbHex := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	rdbBytes, err := hex.DecodeString(emptyRdbHex)
	if err != nil {
		log.Print("Error decoding RDB hex: ", err)
		return
	}

	rdbHeader := fmt.Sprintf("$%d\r\n", len(rdbBytes))

	fullPayload := append([]byte(rdbHeader), rdbBytes...)

	_, err = conn.Write(fullPayload)
	if err != nil {
		log.Print("Error writing RDB file to replica: ", err)
		return
	}

	ReplicasMu.Lock()
	Replicas = append(Replicas, conn)
	ReplicasMu.Unlock()
}
