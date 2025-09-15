package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/ogioldat/olappie/core"
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

	log.Printf("Client connected: %s", conn.RemoteAddr())

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
		log.Printf("Connection error: %v", err)
	}

	log.Printf("Client disconnected: %s", conn.RemoteAddr())
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

	log.Printf("Database server listening on %s", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func main() {
	// Initialize the database
	db := core.NewLSMTStorage()

	// Create and start the server
	server := NewServer(":8080", db)

	log.Println("Starting database server...")
	if err := server.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
