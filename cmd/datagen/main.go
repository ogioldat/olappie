package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/joho/godotenv"
	"github.com/ogioldat/olappie/client"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	var numRecords int
	var valueSize int
	var serverAddr string

	flag.IntVar(&numRecords, "n", 1000, "Number of records to generate")
	flag.IntVar(&valueSize, "size", 64, "Size of generated values in bytes")
	flag.StringVar(&serverAddr, "server", "localhost:8080", "Server address")
	flag.Parse()

	if numRecords <= 0 {
		fmt.Println("Number of records must be positive")
		return
	}

	fmt.Printf("Connecting to server at %s...\n", serverAddr)

	// Connect to the server
	client := client.NewDBClient(serverAddr)
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer client.Disconnect()

	gofakeit.Seed(time.Now().UnixNano())

	start := time.Now()

	for i := 0; i < numRecords; i++ {
		key := strings.ToLower(gofakeit.Word())
		value := string(generateRandomValue(valueSize))

		// Write using client
		if err := client.Write(key, []byte(value)); err != nil {
			fmt.Printf("Error writing record %d: %v\n", i, err)
			return
		}

		if (i+1)%1000 == 0 {
			fmt.Printf("Generated %d records...\n", i+1)
		}
	}

	duration := time.Since(start)
	fmt.Printf("Successfully generated %d records in %v\n", numRecords, duration)
	fmt.Printf("Rate: %.2f records/second\n", float64(numRecords)/duration.Seconds())
}

func generateRandomValue(size int) []byte {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var result strings.Builder
	result.Grow(size)

	for range size {
		result.WriteByte(charset[rand.Intn(len(charset))])
	}

	return []byte(result.String())
}
