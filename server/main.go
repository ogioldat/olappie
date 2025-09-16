package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/ogioldat/olappie/core"
	"github.com/ogioldat/olappie/internal"
)

type Server struct {
	db   core.DB
	addr string
}

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

func NewServer(addr string, db core.DB) *Server {
	return &Server{
		db:   db,
		addr: addr,
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	encoder := json.NewEncoder(conn)

	internal.Logger.Info("Client connected", "addr", conn.RemoteAddr())

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var req Request
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			resp := Response{Success: false, Error: "Invalid JSON"}
			encoder.Encode(resp)
			continue
		}

		resp := s.processRequest(req)
		if err := encoder.Encode(resp); err != nil {
			log.Printf("Error encoding response: %v", err)
			break
		}
	}

	if err := scanner.Err(); err != nil {
		internal.Logger.Info("Connection error", "err", err)
	}

	internal.Logger.Info("Client disconnected", "addr", conn.RemoteAddr())
}

func (s *Server) processRequest(req Request) Response {
	switch strings.ToUpper(req.Operation) {
	case "GET":
		if req.Key == "" {
			return Response{Success: false, Error: "Key required for GET operation"}
		}

		value, err := s.db.Read(req.Key)
		if err != nil {
			return Response{Success: false, Error: err.Error()}
		}

		return Response{Success: true, Data: string(value)}

	case "SET":
		if req.Key == "" {
			return Response{Success: false, Error: "Key required for SET operation"}
		}

		err := s.db.Write(req.Key, []byte(req.Value))
		if err != nil {
			return Response{Success: false, Error: err.Error()}
		}

		return Response{Success: true}

	case "LIST":
		var keys []string

		for key, value := range s.db.Iter {
			keys = append(keys, fmt.Sprintf("%s=%s", key, string(value)))
		}

		data := strings.Join(keys, "\n")
		return Response{Success: true, Data: data}

	default:
		return Response{Success: false, Error: "Unsupported operation: " + req.Operation}
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.addr, err)
	}
	defer listener.Close()

	internal.Logger.Info("Database server listening", "addr", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			internal.Logger.Info("Error accepting connection", "err", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func main() {
	internal.InitLogger()

	// Initialize the database
	db := core.NewLSMTStorage()

	// Create and start the server
	server := NewServer(":8080", db)

	internal.Logger.Info("Starting database server...")
	if err := server.Start(); err != nil {
		internal.Logger.Info("Server failed", "err", err)
	}
}
