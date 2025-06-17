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
	"go.uber.org/zap"

	zap_betterstack "xcode/logger"

	"google.golang.org/grpc"
)

//TODO - Use Zap_BetterStack logger  throughtout this file -add TraceID as well --partiallydone, avoiding repo layer to reduce amount of logs
//TODO - Study and Test all the challenge endpoints and create api doc.
//TODO - psql -snakecase, mongodb - camelcase, fields - pascalcase.

func main() {

	natsClient, err := natsclient.NewNatsClient(configs.LoadConfig().NATSURL)
	if err != nil {
		log.Fatalf("Failed to create NATS client: %v", err)
	}

	config := configs.LoadConfig()

	// Initialize Zap logger based on environment
	var logger *zap.Logger
	if config.Environment == "development" {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		panic("Failed to initialize Zap logger: " + err.Error())
	}
	defer logger.Sync()

	// Initialize BetterStackLogStreamer
	logStreamer := zap_betterstack.NewBetterStackLogStreamer(
		config.BetterStackSourceToken,
		config.Environment,
		config.BetterStackUploadURL,
		logger,
	)

	redisCacheClient := cache.NewRedisCache(config.RedisURL, "", 0)

	mongoclientInstance := mongoconn.ConnectDB()

	// Initialize RedisBoard Leaderboard
	lbConfig := redisboard.Config{
		Namespace:   "problems_leaderboard",
		K:           10,
		MaxUsers:    1_000_000,
		MaxEntities: 200,
		FloatScores: true,
		RedisAddr:   config.RedisURL,
	}
	lb, err := redisboard.New(lbConfig)
	if err != nil {
		log.Fatalf("Failed to initialize leaderboard: %v", err)
	}
	defer lb.Close()

	repoInstance := repository.NewRepository(mongoclientInstance, lb, logStreamer)

	serviceInstance := service.NewService(*repoInstance, natsClient, *redisCacheClient, lb, logStreamer)

	serviceInstance.StartCronJob() //NON Blocking cron for periodically syncing leaderboards.

	// Start gRPC server
	lis, err := net.Listen("tcp", ":"+config.ProblemService)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", config.ProblemService, err)
	}

	grpcServer := grpc.NewServer()
	problemService.RegisterProblemsServiceServer(grpcServer, serviceInstance)

	log.Printf("ProblemService gRPC server running on port %s", config.ProblemService) //50055
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}

}
