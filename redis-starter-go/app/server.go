package main

import (
	"bufio"
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

var ServerMemory sync.Map
var UserRegistry sync.Map

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
			return
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
	}
	content = strings.TrimSpace(content)
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(content), content)
	_, err = conn.Write([]byte(response))
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
	}
	key = strings.TrimSpace(key)

	_, _ = reader.ReadString('\n')
	value, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading value: %v", err)
	}
	value = strings.TrimSpace(value)
	ServerMemory.Store(key, value)

	_, err = conn.Write([]byte("+OK\r\n"))

	if reader.Buffered() > 0 {
		_, _ = reader.ReadString('\n')
		option, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading the option: %v", err)
		}
		option = strings.ToUpper(strings.TrimSpace(option))

		if option == "PX" {
			_, _ = reader.ReadString('\n')
			tte, err := reader.ReadString('\n')
			tteNum, _ := strconv.Atoi(strings.TrimSpace(tte))
			if err != nil {
				log.Printf("Error reading time to expire: %v", err)
			}

			go func(tte int) {
				time.Sleep(time.Millisecond * time.Duration(tte))
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
	}
	key = strings.TrimSpace(key)
	log.Print(key)
	value, ok := ServerMemory.Load(key)
	if !ok {
		_, err = conn.Write([]byte("$-1\r\n"))
	} else {
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(value.(string)), value.(string))
		_, err = conn.Write([]byte(response))
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
	}
	cmd = strings.ToUpper(strings.TrimSpace(cmd))

	switch cmd {
	case "WHOAMI":
		log.Print("user: ", *authUser)
		_, err = conn.Write([]byte("$" + strconv.Itoa(len(*authUser)) + "\r\n" + *authUser + "\r\n"))
		if err != nil {
			log.Print("Writing error: ", err)
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
		}

	case "SETUSER":
		_, _ = reader.ReadString('\n')
		username, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading the username: %v", err)
		}
		username = strings.TrimSpace(username)

		_, _ = reader.ReadString('\n')
		arg, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading the argument: %v", err)
		}
		if strings.HasPrefix(arg, ">") {
			password, _ := strings.CutPrefix(arg, ">")
			password = strings.TrimSpace(password)
			hashBytes := sha256.Sum256([]byte(password))
			hashString := hex.EncodeToString(hashBytes[:])
			val, ok := UserRegistry.Load(username)
			if !ok {
				log.Printf("User %s not found", username)
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
	}
	username = strings.TrimSpace(username)

	_, _ = reader.ReadString('\n')
	password, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading the username: %v", err)
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

func checkAuth(authUser *string) bool {
	return *authUser != ""
}
