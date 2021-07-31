package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Clusternodes []string
	PSQL         string
	Keyspace     string
	Timeout      int
	Username     string
	Password     string
	Patterns     []string
}

func ReadConfig(filename string) Config {
	file, _ := os.Open(filename)
	decoder := json.NewDecoder(file)
	config := Config{}
	err := decoder.Decode(&config)
	if err != nil {
		fmt.Println("Error reading configuration: ", err)
	}
	return config
}
