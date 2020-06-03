package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
)

type KVClient interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
}

type Client struct {
	conn net.Conn
	bufReader *bufio.Reader
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn, bufReader: bufio.NewReader(conn)}, nil
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	b := bytes.Buffer{}
	b.WriteString("GET ")
	b.WriteString(key)
	b.WriteString("\n")

	_, err := c.conn.Write(b.Bytes())
	if err != nil {
		return nil, err
	}

	bytes, err := c.bufReader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	response, err := parseResponse(bytes)
	if err != nil {
		return nil, err
	}

	if !response.IsOK {
		return nil, fmt.Errorf("error occured: %s", string(response.Payload))
	}

	return response.Payload, nil
}

func (c *Client) Set(ctx context.Context, key string, value []byte) error {
	b := bytes.Buffer{}
	b.WriteString("SET ")
	b.WriteString(key)
	b.WriteString(" ")
	b.Write(value)
	b.WriteString("\n")

	_, err := c.conn.Write(b.Bytes())
	if err != nil {
		return err
	}

	bytes, err := c.bufReader.ReadBytes('\n')
	if err != nil {
		return err
	}

	response, err := parseResponse(bytes)
	if err != nil {
		return err
	}

	if !response.IsOK {
		return fmt.Errorf("error: %s", string(response.Payload))
	}

	return nil
}

type Response struct {
	IsOK bool
	Payload []byte
}

func parseResponse(res []byte) (*Response, error) {
	bytesSplitted := bytes.SplitN(res, []byte(" "), 2)

	status := bytes.TrimSpace(bytesSplitted[0])

	if bytes.Equal(status, []byte("ERROR")) {
		return &Response{IsOK: false, Payload: bytesSplitted[1]}, nil
	}

	if bytes.Equal(status, []byte("OK")) {
		var payload []byte
		if len(bytesSplitted) >= 2 {
			payload = bytes.TrimSpace(bytesSplitted[1])
		} else {
			payload = nil
		}
		return &Response{IsOK: true, Payload: payload}, nil
	}

	return nil, fmt.Errorf("unknown format")
}
