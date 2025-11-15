package main

import (
	"fmt"
	"log"

	"github.com/esadakcam/conductor/internal/task"
)

func main() {
	fmt.Println("Hello, World!")

	config, err := task.LoadConfig("/Users/esad/Projects/conductor/config/config.example.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	for _, task := range config.Tasks {
		fmt.Println(task)
		fmt.Println(task.When)
		fmt.Println(task.Then)
	}
}
