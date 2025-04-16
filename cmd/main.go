package main

import (
	"log"
	"net"
	"xcode/cache"
	configs "xcode/config"
	"xcode/mongoconn"
	"xcode/natsclient"
	"xcode/repository"
	"xcode/service"

	problemService "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	redisboard "github.com/lijuuu/RedisBoard"

	"google.golang.org/grpc"
)

func main() {

	natsClient, err := natsclient.NewNatsClient(configs.LoadConfig().NATSURL)
	if err != nil {
		log.Fatalf("Failed to create NATS client: %v", err)
	}

	configValues := configs.LoadConfig()

	redisCacheClient := cache.NewRedisCache(configValues.RedisURL, "", 0)

	mongoclientInstance := mongoconn.ConnectDB()

	// Initialize RedisBoard Leaderboard
	lbConfig := redisboard.Config{
		Namespace:   "problems_leaderboard",
		K:           10,
		MaxUsers:    1_000_000,
		MaxEntities: 200,
		FloatScores: true,
		RedisAddr:   configValues.RedisURL, 
	}
	lb, err := redisboard.New(lbConfig)
	if err != nil {
		log.Fatalf("Failed to initialize leaderboard: %v", err)
	}
	defer lb.Close()

	repoInstance := repository.NewRepository(mongoclientInstance,lb)

	serviceInstance := service.NewService(*repoInstance, natsClient, *redisCacheClient,lb)

	// Start gRPC server
	lis, err := net.Listen("tcp", ":"+configValues.ProblemService)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", configValues.ProblemService, err)
	}

	grpcServer := grpc.NewServer()
	problemService.RegisterProblemsServiceServer(grpcServer, serviceInstance)

	log.Printf("ProblemService gRPC server running on port %s", configValues.ProblemService) //50055
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}

}
