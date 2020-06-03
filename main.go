package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"strings"
)

type KV interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
}

var kv KV

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	kv = NewStorageKV()

	ln, err := net.Listen("tcp", ":16379")
	if err != nil {
		logrus.Fatal(err)
	}
	defer ln.Close()

	logrus.Info("server is running")

	for {
		conn, err := ln.Accept()
		if err != nil {
			logrus.Error(err)
		}
		logrus.Debug("connection established")

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	rconn := bufio.NewReader(conn)
	for {
		req, err := rconn.ReadBytes('\n')
		if err == io.EOF {
			logrus.Debug("connection closed")
			conn.Close()
			return
		}

		if err != nil {
			logrus.Error(err)
			fmt.Fprintf(conn, "ERROR %s\n", err.Error())
			continue
		}

		logrus.Debugf("input: %s", hex.EncodeToString(req))

		reqBytes := bytes.SplitN(req, []byte{' '}, 2)
		if len(reqBytes) != 2 {
			logrus.Errorf("invalid format: split to %d", len(reqBytes))
			fmt.Fprintf(conn, "ERROR invalid format\n")
			continue
		}
		operation := string(reqBytes[0])
		message := reqBytes[1]

		switch operation {
		case "GET":
			handleGetRequest(conn, message)
		case "SET":
			handleSetRequest(conn, message)
		default:
			logrus.Errorf("Unknown operation: %s", operation)
			fmt.Fprintf(conn, "ERROR unknown operation\n")
		}
	}
}

func handleGetRequest(conn net.Conn, message []byte) {
	key := strings.TrimSpace(string(message))
	value, err := kv.Get(context.Background(), key)
	if err != nil {
		logrus.Error(err)
		fmt.Fprintf(conn, "ERROR %s\n", err.Error())
		return
	}

	conn.Write([]byte("OK "))
	conn.Write(value)
	conn.Write([]byte("\n"))
	return
}

func handleSetRequest(conn net.Conn, message []byte) {
	messageBytes := bytes.SplitN(message, []byte{' '}, 2)
	if len(messageBytes) != 2 {
		logrus.Error("invalid format")
		fmt.Fprintf(conn, "ERROR invalid format\n")
		return
	}

	key := string(messageBytes[0])
	value := bytes.TrimSpace(messageBytes[1])

	err := kv.Set(context.Background(), key, value)
	if err != nil {
		logrus.Error(err)
		fmt.Fprintf(conn, "ERROR %s\n", err.Error())
		return
	}

	conn.Write([]byte("OK\n"))
}
