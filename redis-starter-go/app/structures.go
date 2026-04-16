package main

import (
	"net"
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

type Replica struct {
	Conn   net.Conn
	Offset int
}

type SilentConn struct {
	net.Conn
}

func (s SilentConn) Write(b []byte) (int, error) {
	return len(b), nil
}

type SortedSetElement struct {
	Name  string
	Score float64
}

type SortedSet struct {
	sync.RWMutex
	Elements map[string]float64
	Order    []SortedSetElement
}
