package tests

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

func SetupTestDB() {
	// Load environment variables from .env.test file
	log.Println("Current working directory:")
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	err = godotenv.Load("../.env.test")
	if err != nil {
		log.Fatal("Error loading .env.test file")
	}
}
