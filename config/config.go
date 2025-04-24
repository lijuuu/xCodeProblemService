package configs

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	APIGATEWAYPORT string
	UserGRPCPort   string
	MongoDBURL     string
	ProblemService string
	NATSURL        string
	RedisURL       string
}

func LoadConfig() Config {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file", err)
	}
	config := Config{
		APIGATEWAYPORT: getEnv("APIGATEWAYPORT", "7000"),
		UserGRPCPort:   getEnv("USERGRPCPORT", "50051"),
		MongoDBURL:     getEnv("MONGODBURL", "mongodb://localhost:27017"),
		ProblemService: getEnv("PROBLEMSERVICE", "50055"),
		NATSURL:        getEnv("NATSURL", "nats://localhost:4222"),
		RedisURL:       getEnv("REDISURL", "localhost:6379"),
	}

	// fmt.Println(config)
	return config
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
