package client

import (
	"encoding/json"
	"fmt"
	"net"
)

type Request struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
}

type Response struct {
	Success bool   `json:"success"`
	Data    string `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
}

type DBClient struct {
	serverAddr string
	conn       net.Conn
	encoder    *json.Encoder
	decoder    *json.Decoder
}

func NewDBClient(serverAddr string) *DBClient {
	return &DBClient{serverAddr: serverAddr}
}

func (c *DBClient) Connect() error {
	if c.conn != nil {
		return nil
	}

	conn, err := net.Dial("tcp", c.serverAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %v", err)
	}

	c.conn = conn
	c.encoder = json.NewEncoder(conn)
	c.decoder = json.NewDecoder(conn)
	return nil
}

func (c *DBClient) Disconnect() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.encoder = nil
		c.decoder = nil
		return err
	}
	return nil
}

func (c *DBClient) Read(key string) ([]byte, error) {
	req := Request{
		Operation: "GET",
		Key:       key,
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	return []byte(resp.Data), nil
}

func (c *DBClient) Write(key string, value []byte) error {
	req := Request{
		Operation: "SET",
		Key:       key,
		Value:     string(value),
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}

	return nil
}

func (c *DBClient) List() (string, error) {
	req := Request{
		Operation: "LIST",
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return "", err
	}

	if !resp.Success {
		return "", fmt.Errorf("%s", resp.Error)
	}

	return resp.Data, nil
}

func (c *DBClient) sendRequest(req Request) (*Response, error) {
	if c.conn == nil {
		if err := c.Connect(); err != nil {
			return nil, err
		}
	}

	if err := c.encoder.Encode(req); err != nil {
		c.Disconnect()
		if err := c.Connect(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %v", err)
		}
		if err := c.encoder.Encode(req); err != nil {
			return nil, fmt.Errorf("failed to send request: %v", err)
		}
	}

	var resp Response
	if err := c.decoder.Decode(&resp); err != nil {
		c.Disconnect()
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	return &resp, nil
}
