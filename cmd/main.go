package main

import (
	"log"
	"net"
	configs "xcode/config"
	"xcode/mongoconn"
	"xcode/repository"
	"xcode/service"
	problemService "github.com/lijuuu/GlobalProtoXcode/ProblemsService"


	"google.golang.org/grpc"
)

func main() {
	mongoclientInstance := mongoconn.ConnectDB()

	repoInstance := repository.NewRepository(mongoclientInstance)

	serviceInstance := service.NewService(repoInstance)

	configValues := configs.LoadConfig()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":"+configValues.ProblemService)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", configValues.ProblemService, err)
	}

	grpcServer := grpc.NewServer()
	problemService.RegisterProblemsServiceServer(grpcServer, serviceInstance)

	log.Printf("ProblemService gRPC server running on port %s", configValues.ProblemService)  //50055
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}

}
