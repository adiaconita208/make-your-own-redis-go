package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func LoadRDB(dir, filename string) {
	if dir == "" || filename == "" {
		return
	}

	path := filepath.Join(dir, filename)
	file, err := os.Open(path)
	if err != nil {
		log.Print("File could not be opened: ", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// 9-byte header
	_, err = reader.Discard(9)
	if err != nil {
		log.Print("Reading error: ", err)
		return
	}

	for {
		b, err := reader.ReadByte()
		if err != nil {
			log.Print("Error reading byte: ", err)
			return
		}

		switch b {
		case 0xFF: // EOF byte
			return

		case 0xFA: // Metadata
			_, _ = readString(reader) // name
			_, _ = readString(reader) // value

		case 0xFE: // Database selector
			_, _, _ = readLength(reader) // database index

		case 0xFB: // Hash table sizes
			_, _, _ = readLength(reader) // hash size
			_, _, _ = readLength(reader) // tte size

		case 0xFC: // TTE in milliseconds
			var tteMs uint64
			_ = binary.Read(reader, binary.LittleEndian, &tteMs)

			_, _ = reader.ReadByte()

			key, err := readString(reader)
			if err != nil {
				log.Print("Error reading string: ", err)
				return
			}

			val, err := readString(reader)
			if err != nil {
				log.Print("Error reading string: ", err)
				return
			}

			expiryTime := time.UnixMilli(int64(tteMs))

			timeLeft := time.Until(expiryTime)
			if timeLeft <= 0 {
				continue
			}

			ServerMemory.Store(key, val)

			go func(duration time.Duration, key string) {
				ctx, cancel := context.WithTimeout(context.Background(), duration)
				defer cancel()
				<-ctx.Done()
				ServerMemory.Delete(key)
			}(timeLeft, key)

		case 0xFD: // TTE in seconds
			var tteSec uint64
			_ = binary.Read(reader, binary.LittleEndian, &tteSec)
			_, _ = reader.ReadByte()

			key, err := readString(reader)
			if err != nil {
				log.Print("Error reading string: ", err)
				return
			}

			val, err := readString(reader)
			if err != nil {
				log.Print("Error reading string: ", err)
				return
			}

			expiryTime := time.Unix(int64(tteSec), 0)
			timeLeft := time.Until(expiryTime)

			if timeLeft <= 0 {
				continue
			}

			ServerMemory.Store(key, val)

			go func(duration time.Duration, key string) {
				ctx, cancel := context.WithTimeout(context.Background(), duration)
				defer cancel()
				<-ctx.Done()
				ServerMemory.Delete(key)
			}(timeLeft, key)

		default: //Value type and no TTE
			key, err := readString(reader)
			if err != nil {
				log.Print("Error reading string: ", err)
				return
			}

			val, err := readString(reader)
			if err != nil {
				log.Print("Error reading string: ", err)
				return
			}

			ServerMemory.Store(key, val)

		}
	}
}

func readString(reader *bufio.Reader) (string, error) {
	length, isSpecial, err := readLength(reader)
	if err != nil {
		return "", err
	}

	if isSpecial {
		switch length {
		case 0: // 8-bit integer
			var val int8
			_ = binary.Read(reader, binary.LittleEndian, &val)
			return strconv.Itoa(int(val)), nil

		case 1: // 16-bit Integer (Little Endian)
			var val uint16
			_ = binary.Read(reader, binary.LittleEndian, &val)
			return strconv.Itoa(int(val)), nil

		case 2: // 32-bit Integer (Little Endian)
			var val uint32
			_ = binary.Read(reader, binary.LittleEndian, &val)
			return strconv.Itoa(int(val)), nil
		}
	}

	strBytes := make([]byte, length)
	_, err = io.ReadFull(reader, strBytes)
	return string(strBytes), err
}

func readLength(reader *bufio.Reader) (uint32, bool, error) {
	b, err := reader.ReadByte()
	if err != nil {
		log.Print("Error reading byte: ", err)
		return 0, false, err
	}

	format := (b & 0xC0) >> 6 // first 2 bits
	value := b & 0x3F         // last 6 bits

	switch format {
	case 0: // Next 6 bits represent the length
		return uint32(value), false, nil

	case 1: // Read one more byte and the value will be last 6 bits + 8 new bits
		nextByte, err := reader.ReadByte()
		if err != nil {
			log.Print("Error reading byte: ", err)
			return 0, false, err
		}

		length := (uint32(value) << 8) | uint32(nextByte)
		return length, false, nil

	case 2: // Discard the 6 bits, Next 4 bytes is a Big Endian Integer
		var length uint32
		_ = binary.Read(reader, binary.BigEndian, &length)
		return length, false, nil

	case 3: // Special string
		return uint32(value), true, nil
	}

	return 0, false, nil
}
