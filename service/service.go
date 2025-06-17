package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"xcode/cache"
	"xcode/model"
	"xcode/natsclient"
	"xcode/repository"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	redisboard "github.com/lijuuu/RedisBoard"
	cron "github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zap_betterstack "xcode/logger"

	"github.com/google/uuid"
)

// ProblemService handles problem-related operations
type ProblemService struct {
	RepoConnInstance repository.Repository
	NatsClient       *natsclient.NatsClient
	RedisCacheClient cache.RedisCache
	LB               *redisboard.Leaderboard
	pb.UnimplementedProblemsServiceServer
	logger *zap_betterstack.BetterStackLogStreamer
}

func NewService(repo repository.Repository, natsClient *natsclient.NatsClient, redisCache cache.RedisCache, lb *redisboard.Leaderboard, logger *zap_betterstack.BetterStackLogStreamer) *ProblemService {
	traceID := uuid.New().String()
	svc := &ProblemService{
		RepoConnInstance: repo,
		NatsClient:       natsClient,
		RedisCacheClient: redisCache,
		LB:               lb,
		logger:           logger,
	}
	// Sync leaderboard during initialization
	if err := svc.SyncLeaderboardFromMongo(context.Background()); err != nil {
		svc.logger.Log(zapcore.ErrorLevel, traceID, "Failed to sync leaderboard during service initialization", map[string]any{
			"method":    "NewService",
			"errorType": "LEADERBOARD_SYNC_FAILED",
		}, "SERVICE", err)
	}
	svc.logger.Log(zapcore.InfoLevel, traceID, "ProblemService initialized", map[string]any{
		"method": "NewService",
	}, "SERVICE", nil)
	return svc
}

func (s *ProblemService) SyncLeaderboardFromMongo(ctx context.Context) error {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting SyncLeaderboardFromMongo", map[string]any{
		"method": "SyncLeaderboardFromMongo",
	}, "SERVICE", nil)

	err := s.RepoConnInstance.SyncLeaderboardToRedis(ctx)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to sync leaderboard to Redis", map[string]any{
			"method":    "SyncLeaderboardFromMongo",
			"errorType": "LEADERBOARD_SYNC_FAILED",
		}, "SERVICE", err)
		return err
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Leaderboard synced successfully", map[string]any{
		"method": "SyncLeaderboardFromMongo",
	}, "SERVICE", nil)
	return nil
}

func (s *ProblemService) StartCronJob() {
	c := cron.New()

	// schedule leaderboard sync every hour
	c.AddFunc("@every 1h", func() {
		ctx := context.Background()
		s.logger.Log(zapcore.InfoLevel, "", "Syncing MongoDB Submissions and RedisBoard "+time.Now().String(), map[string]any{
			"method": "SYNC LEADERBOARD CRON JOB",
		}, "SERVICE", nil)
		s.SyncLeaderboardFromMongo(ctx)
	})

	c.Start() // ⚠️ this does NOT block
}

// GetService returns the ProblemService instance
func (s *ProblemService) GetService() *ProblemService {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Returning ProblemService instance", map[string]any{
		"method": "GetService",
	}, "SERVICE", nil)
	return s
}

// createGrpcError constructs a gRPC error
func (s *ProblemService) createGrpcError(code codes.Code, message string, errorType string, cause error) error {
	traceID := uuid.New().String()
	details := message
	if cause != nil {
		details = cause.Error()
	}
	s.logger.Log(zapcore.ErrorLevel, traceID, "Creating gRPC error", map[string]any{
		"method":    "createGrpcError",
		"code":      code,
		"errorType": errorType,
		"details":   details,
	}, "SERVICE", nil)
	return status.Error(code, fmt.Sprintf("ErrorType: %s, Code: %d, Details: %s", errorType, code, details))
}

// CreateProblem creates a new problem
func (s *ProblemService) CreateProblem(ctx context.Context, req *pb.CreateProblemRequest) (*pb.CreateProblemResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting CreateProblem", map[string]any{
		"method":       "CreateProblem",
		"problemTitle": req.Title,
	}, "SERVICE", nil)

	if req.Title == "" || req.Description == "" || req.Difficulty == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing required fields", map[string]any{
			"method":    "CreateProblem",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Title, description, and difficulty are required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.CreateProblem(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to create problem", map[string]any{
			"method":       "CreateProblem",
			"problemTitle": req.Title,
			"errorType":    "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	cacheKey := "problems_list:*"
	if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
			"method":    "CreateProblem",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Problem created successfully", map[string]any{
		"method":       "CreateProblem",
		"problemTitle": req.Title,
	}, "SERVICE", nil)
	return resp, nil
}

// UpdateProblem updates an existing problem
func (s *ProblemService) UpdateProblem(ctx context.Context, req *pb.UpdateProblemRequest) (*pb.UpdateProblemResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting UpdateProblem", map[string]any{
		"method":    "UpdateProblem",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)

	if req.ProblemId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID", map[string]any{
			"method":    "UpdateProblem",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.UpdateProblem(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to update problem", map[string]any{
			"method":    "UpdateProblem",
			"problemId": req.ProblemId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	cacheKeys := []string{
		fmt.Sprintf("problem:%s", req.ProblemId),
		fmt.Sprintf("problem_slug:%s", *req.Title),
		"problems_list:*",
	}
	for _, cacheKey := range cacheKeys {
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":    "UpdateProblem",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Problem updated successfully", map[string]any{
		"method":    "UpdateProblem",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)
	return resp, nil
}

// DeleteProblem deletes a problem
func (s *ProblemService) DeleteProblem(ctx context.Context, req *pb.DeleteProblemRequest) (*pb.DeleteProblemResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting DeleteProblem", map[string]any{
		"method":    "DeleteProblem",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)

	if req.ProblemId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID", map[string]any{
			"method":    "DeleteProblem",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.DeleteProblem(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete problem", map[string]any{
			"method":    "DeleteProblem",
			"problemId": req.ProblemId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	cacheKeys := []string{
		fmt.Sprintf("problem:%s", req.ProblemId),
		"problems_list:*",
	}
	for _, cacheKey := range cacheKeys {
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":    "DeleteProblem",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Problem deleted successfully", map[string]any{
		"method":    "DeleteProblem",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)
	return resp, nil
}

// GetProblem retrieves a problem by ID
func (s *ProblemService) GetProblem(ctx context.Context, req *pb.GetProblemRequest) (*pb.GetProblemResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetProblem", map[string]any{
		"method":    "GetProblem",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)

	if req.ProblemId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID", map[string]any{
			"method":    "GetProblem",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
	cachedProblem, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedProblem != nil {
		var problem pb.GetProblemResponse
		cachedStr, ok := cachedProblem.(string)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to assert cached problem to string", map[string]any{
				"method":    "GetProblem",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", nil)
		} else if err := json.Unmarshal([]byte(cachedStr), &problem); err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Problem retrieved from cache", map[string]any{
				"method":    "GetProblem",
				"problemId": req.ProblemId,
				"cacheKey":  cacheKey,
			}, "SERVICE", nil)
			return &problem, nil
		}
	}

	problemRepoModel, err := s.RepoConnInstance.GetProblem(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to retrieve problem from DB", map[string]any{
			"method":    "GetProblem",
			"problemId": req.ProblemId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	problemPB := repository.ToProblemResponse(*problemRepoModel)
	problemBytes, err := json.Marshal(problemPB)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal problem", map[string]any{
			"method":    "GetProblem",
			"problemId": req.ProblemId,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, problemBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache problem", map[string]any{
			"method":    "GetProblem",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Problem retrieved successfully", map[string]any{
		"method":    "GetProblem",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)
	return problemPB, nil
}

// ListProblems retrieves a paginated list of problems
func (s *ProblemService) ListProblems(ctx context.Context, req *pb.ListProblemsRequest) (*pb.ListProblemsResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting ListProblems", map[string]any{
		"method":   "ListProblems",
		"page":     req.Page,
		"pageSize": req.PageSize,
	}, "SERVICE", nil)

	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = 10
	}

	cacheKey := fmt.Sprintf("problems_list:%d:%d", req.Page, req.PageSize)
	cachedProblems, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedProblems != nil {
		var problems pb.ListProblemsResponse
		cachedStr, ok := cachedProblems.(string)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to assert cached problems to string", map[string]any{
				"method":    "ListProblems",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", nil)
		} else if err := json.Unmarshal([]byte(cachedStr), &problems); err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Problems list retrieved from cache", map[string]any{
				"method":   "ListProblems",
				"cacheKey": cacheKey,
				"page":     req.Page,
				"pageSize": req.PageSize,
			}, "SERVICE", nil)
			return &problems, nil
		}
	}

	resp, err := s.RepoConnInstance.ListProblems(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to retrieve problems list from DB", map[string]any{
			"method":    "ListProblems",
			"page":      req.Page,
			"pageSize":  req.PageSize,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	problemsBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal problems list", map[string]any{
			"method":    "ListProblems",
			"page":      req.Page,
			"pageSize":  req.PageSize,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, problemsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache problems list", map[string]any{
			"method":    "ListProblems",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Problems list retrieved successfully", map[string]any{
		"method":   "ListProblems",
		"page":     req.Page,
		"pageSize": req.PageSize,
	}, "SERVICE", nil)
	return resp, nil
}

// AddTestCases adds test cases to a problem
func (s *ProblemService) AddTestCases(ctx context.Context, req *pb.AddTestCasesRequest) (*pb.AddTestCasesResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting AddTestCases", map[string]any{
		"method":    "AddTestCases",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)

	if req.ProblemId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID", map[string]any{
			"method":    "AddTestCases",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	if len(req.Testcases.Run) == 0 && len(req.Testcases.Submit) == 0 {
		s.logger.Log(zapcore.ErrorLevel, traceID, "No test cases provided", map[string]any{
			"method":    "AddTestCases",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "At least one test case is required", "VALIDATION_ERROR", nil)
	}
	for _, tc := range req.Testcases.Run {
		if tc.Input == "" || tc.Expected == "" {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Invalid test case input or expected output", map[string]any{
				"method":    "AddTestCases",
				"errorType": "VALIDATION_ERROR",
			}, "SERVICE", nil)
			return nil, s.createGrpcError(codes.InvalidArgument, "Test case input and expected output are required", "VALIDATION_ERROR", nil)
		}
	}
	for _, tc := range req.Testcases.Submit {
		if tc.Input == "" || tc.Expected == "" {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Invalid test case input or expected output", map[string]any{
				"method":    "AddTestCases",
				"errorType": "VALIDATION_ERROR",
			}, "SERVICE", nil)
			return nil, s.createGrpcError(codes.InvalidArgument, "Test case input and expected output are required", "VALIDATION_ERROR", nil)
		}
	}

	resp, err := s.RepoConnInstance.AddTestCases(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to add test cases", map[string]any{
			"method":    "AddTestCases",
			"problemId": req.ProblemId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
	if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
			"method":    "AddTestCases",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Test cases added successfully", map[string]any{
		"method":    "AddTestCases",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)
	return resp, nil
}

// AddLanguageSupport adds language support to a problem
func (s *ProblemService) AddLanguageSupport(ctx context.Context, req *pb.AddLanguageSupportRequest) (*pb.AddLanguageSupportResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting AddLanguageSupport", map[string]any{
		"method":    "AddLanguageSupport",
		"problemId": req.ProblemId,
		"language":  req.Language,
	}, "SERVICE", nil)

	if req.ProblemId == "" || req.Language == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID or language", map[string]any{
			"method":    "AddLanguageSupport",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}
	if req.ValidationCode == nil || req.ValidationCode.Code == "" || req.ValidationCode.Template == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing validation code or template", map[string]any{
			"method":    "AddLanguageSupport",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Validation code (code and template) is required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.AddLanguageSupport(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to add language support", map[string]any{
			"method":    "AddLanguageSupport",
			"problemId": req.ProblemId,
			"language":  req.Language,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	cacheKeys := []string{
		fmt.Sprintf("problem:%s", req.ProblemId),
		fmt.Sprintf("language_supports:%s", req.ProblemId),
	}
	for _, cacheKey := range cacheKeys {
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":    "AddLanguageSupport",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Language support added successfully", map[string]any{
		"method":    "AddLanguageSupport",
		"problemId": req.ProblemId,
		"language":  req.Language,
	}, "SERVICE", nil)
	return resp, nil
}

// UpdateLanguageSupport updates language support for a problem
func (s *ProblemService) UpdateLanguageSupport(ctx context.Context, req *pb.UpdateLanguageSupportRequest) (*pb.UpdateLanguageSupportResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting UpdateLanguageSupport", map[string]any{
		"method":    "UpdateLanguageSupport",
		"problemId": req.ProblemId,
		"language":  req.Language,
	}, "SERVICE", nil)

	if req.ProblemId == "" || req.Language == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID or language", map[string]any{
			"method":    "UpdateLanguageSupport",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}
	if req.ValidationCode == nil || req.ValidationCode.Code == "" || req.ValidationCode.Template == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing validation code or template", map[string]any{
			"method":    "UpdateLanguageSupport",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Validation code (code and template) is required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.UpdateLanguageSupport(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to update language support", map[string]any{
			"method":    "UpdateLanguageSupport",
			"problemId": req.ProblemId,
			"language":  req.Language,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	cacheKeys := []string{
		fmt.Sprintf("problem:%s", req.ProblemId),
		fmt.Sprintf("language_supports:%s", req.ProblemId),
	}
	for _, cacheKey := range cacheKeys {
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":    "UpdateLanguageSupport",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Language support updated successfully", map[string]any{
		"method":    "UpdateLanguageSupport",
		"problemId": req.ProblemId,
		"language":  req.Language,
	}, "SERVICE", nil)
	return resp, nil
}

// RemoveLanguageSupport removes language support from a problem
func (s *ProblemService) RemoveLanguageSupport(ctx context.Context, req *pb.RemoveLanguageSupportRequest) (*pb.RemoveLanguageSupportResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting RemoveLanguageSupport", map[string]any{
		"method":    "RemoveLanguageSupport",
		"problemId": req.ProblemId,
		"language":  req.Language,
	}, "SERVICE", nil)

	if req.ProblemId == "" || req.Language == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID or language", map[string]any{
			"method":    "RemoveLanguageSupport",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.RemoveLanguageSupport(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to remove language support", map[string]any{
			"method":    "RemoveLanguageSupport",
			"problemId": req.ProblemId,
			"language":  req.Language,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	cacheKeys := []string{
		fmt.Sprintf("problem:%s", req.ProblemId),
		fmt.Sprintf("language_supports:%s", req.ProblemId),
	}
	for _, cacheKey := range cacheKeys {
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":    "RemoveLanguageSupport",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Language support removed successfully", map[string]any{
		"method":    "RemoveLanguageSupport",
		"problemId": req.ProblemId,
		"language":  req.Language,
	}, "SERVICE", nil)
	return resp, nil
}

// DeleteTestCase deletes a test case from a problem
func (s *ProblemService) DeleteTestCase(ctx context.Context, req *pb.DeleteTestCaseRequest) (*pb.DeleteTestCaseResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting DeleteTestCase", map[string]any{
		"method":     "DeleteTestCase",
		"problemId":  req.ProblemId,
		"testcaseId": req.TestcaseId,
	}, "SERVICE", nil)

	if req.ProblemId == "" || req.TestcaseId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID or testcase ID", map[string]any{
			"method":    "DeleteTestCase",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and testcase ID are required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.DeleteTestCase(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete test case", map[string]any{
			"method":     "DeleteTestCase",
			"problemId":  req.ProblemId,
			"testcaseId": req.TestcaseId,
			"errorType":  "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
	if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
			"method":    "DeleteTestCase",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Test case deleted successfully", map[string]any{
		"method":     "DeleteTestCase",
		"problemId":  req.ProblemId,
		"testcaseId": req.TestcaseId,
	}, "SERVICE", nil)
	return resp, nil
}

// GetLanguageSupports retrieves supported languages for a problem
func (s *ProblemService) GetLanguageSupports(ctx context.Context, req *pb.GetLanguageSupportsRequest) (*pb.GetLanguageSupportsResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetLanguageSupports", map[string]any{
		"method":    "GetLanguageSupports",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)

	if req.ProblemId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID", map[string]any{
			"method":    "GetLanguageSupports",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("language_supports:%s", req.ProblemId)
	cachedLangs, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedLangs != nil {
		var langs pb.GetLanguageSupportsResponse
		cachedStr, ok := cachedLangs.(string)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to assert cached languages to string", map[string]any{
				"method":    "GetLanguageSupports",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", nil)
		} else if err := json.Unmarshal([]byte(cachedStr), &langs); err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Language supports retrieved from cache", map[string]any{
				"method":    "GetLanguageSupports",
				"problemId": req.ProblemId,
				"cacheKey":  cacheKey,
			}, "SERVICE", nil)
			return &langs, nil
		}
	}

	resp, err := s.RepoConnInstance.GetLanguageSupports(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to retrieve language supports from DB", map[string]any{
			"method":    "GetLanguageSupports",
			"problemId": req.ProblemId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	langsBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal language supports", map[string]any{
			"method":    "GetLanguageSupports",
			"problemId": req.ProblemId,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, langsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache language supports", map[string]any{
			"method":    "GetLanguageSupports",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Language supports retrieved successfully", map[string]any{
		"method":    "GetLanguageSupports",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)
	return resp, nil
}

// FullValidationByProblemID validates a problem across all supported languages
func (s *ProblemService) FullValidationByProblemID(ctx context.Context, req *pb.FullValidationByProblemIDRequest) (*pb.FullValidationByProblemIDResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting FullValidationByProblemID", map[string]any{
		"method":    "FullValidationByProblemID",
		"problemId": req.ProblemId,
	}, "SERVICE", nil)

	if req.ProblemId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID", map[string]any{
			"method":    "FullValidationByProblemID",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return &pb.FullValidationByProblemIDResponse{
			Success:   false,
			Message:   "Problem ID is required",
			ErrorType: "VALIDATION_ERROR",
		}, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}

	data, problem, err := s.RepoConnInstance.BasicValidationByProblemID(ctx, req)
	if err != nil || !data.Success {
		errMsg := data.Message
		if errMsg == "" {
			errMsg = "Basic validation failed"
		}
		s.logger.Log(zapcore.ErrorLevel, traceID, "Basic validation failed", map[string]any{
			"method":    "FullValidationByProblemID",
			"problemId": req.ProblemId,
			"errorType": data.ErrorType,
		}, "SERVICE", err)
		s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
		return data, s.createGrpcError(codes.Unimplemented, errMsg, data.ErrorType, err)
	}

	// fmt.Println("supported problems ", problem.ValidateCode)
	// fmt.Println("length and content of supported languages ",len(problem.SupportedLanguages),problem.SupportedLanguages )
	for _, lang := range problem.SupportedLanguages {
		validateCode, ok := problem.ValidateCode[lang]
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "No validation code found for language", map[string]any{
				"method":    "FullValidationByProblemID",
				"problemId": req.ProblemId,
				"language":  lang,
				"errorType": "CONFIGURATION_ERROR",
			}, "SERVICE", nil)
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("No validation code found for language: %s", lang),
				ErrorType: "CONFIGURATION_ERROR",
			}, s.createGrpcError(codes.InvalidArgument, "Missing validation code", "CONFIGURATION_ERROR", nil)
		}

		res, err := s.RunUserCodeProblem(ctx, &pb.RunProblemRequest{
			ProblemId:     req.ProblemId,
			UserCode:      validateCode.Code,
			Language:      lang,
			IsRunTestcase: false,
		})
		if err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Execution failed for language", map[string]any{
				"method":    "FullValidationByProblemID",
				"problemId": req.ProblemId,
				"language":  lang,
				"errorType": "EXECUTION_ERROR",
			}, "SERVICE", err)
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("Execution failed for language %s: %v", lang, err),
				ErrorType: "EXECUTION_ERROR",
			}, s.createGrpcError(codes.Internal, "Execution error", "EXECUTION_ERROR", err)
		}

		var result map[string]any
		if err := json.Unmarshal([]byte(res.Message), &result); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to parse execution result", map[string]any{
				"method":    "FullValidationByProblemID",
				"problemId": req.ProblemId,
				"language":  lang,
				"errorType": "EXECUTION_ERROR",
			}, "SERVICE", err)
			return nil, fmt.Errorf("failed to parse execution result: %w", err)
		}

		overallPass, ok := result["overallPass"].(bool)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "No output received for language", map[string]any{
				"method":    "FullValidationByProblemID",
				"problemId": req.ProblemId,
				"language":  lang,
				"errorType": "EXECUTION_ERROR",
			}, "SERVICE", nil)
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("No output received for language %s", lang),
				ErrorType: "EXECUTION_ERROR",
			}, s.createGrpcError(codes.Internal, "Invalid execution result "+fmt.Sprintf("No output received for language %s", lang), "EXECUTION_ERROR", nil)
		}

		if !overallPass {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Validation failed for language", map[string]any{
				"method":    "FullValidationByProblemID",
				"problemId": req.ProblemId,
				"language":  lang,
				"errorType": "VALIDATION_FAILED",
			}, "SERVICE", nil)
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("Validation failed for language %s", lang),
				ErrorType: "VALIDATION_FAILED",
			}, s.createGrpcError(codes.FailedPrecondition, "Validation failed", "VALIDATION_FAILED", nil)
		}
	}

	status := s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, true)
	message := "Full Validation Successful"
	if !status {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to toggle validation status", map[string]any{
			"method":    "FullValidationByProblemID",
			"problemId": req.ProblemId,
			"errorType": "DB_ERROR",
		}, "SERVICE", nil)
		s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
		message = "Full Validation completed, but failed to toggle status"
	}

	cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
	if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
			"method":    "FullValidationByProblemID",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Full validation completed", map[string]any{
		"method":    "FullValidationByProblemID",
		"problemId": req.ProblemId,
		"status":    status,
	}, "SERVICE", nil)
	return &pb.FullValidationByProblemIDResponse{
		Success:   status,
		Message:   message,
		ErrorType: "",
	}, nil
}

// GetSubmissionsByOptionalProblemID retrieves submissions
func (s *ProblemService) GetSubmissionsByOptionalProblemID(ctx context.Context, req *pb.GetSubmissionsRequest) (*pb.GetSubmissionsResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetSubmissionsByOptionalProblemID", map[string]any{
		"method":    "GetSubmissionsByOptionalProblemID",
		"problemId": *req.ProblemId,
		"userId":    req.UserId,
	}, "SERVICE", nil)

	if *req.ProblemId == "" && req.UserId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID and user ID", map[string]any{
			"method":    "GetSubmissionsByOptionalProblemID",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and user ID are required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("submissions:%s:%s", *req.ProblemId, req.UserId)
	cachedSubmissions, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedSubmissions != nil {
		var submissions pb.GetSubmissionsResponse
		cachedStr, ok := cachedSubmissions.(string)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to assert cached submissions to string", map[string]any{
				"method":    "GetSubmissionsByOptionalProblemID",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", nil)
		} else if err := json.Unmarshal([]byte(cachedStr), &submissions); err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Submissions retrieved from cache", map[string]any{
				"method":    "GetSubmissionsByOptionalProblemID",
				"problemId": *req.ProblemId,
				"userId":    req.UserId,
				"cacheKey":  cacheKey,
			}, "SERVICE", nil)
			return &submissions, nil
		}
	}

	resp, err := s.RepoConnInstance.GetSubmissionsByOptionalProblemID(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to retrieve submissions from DB", map[string]any{
			"method":    "GetSubmissionsByOptionalProblemID",
			"problemId": *req.ProblemId,
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	submissionsBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal submissions", map[string]any{
			"method":    "GetSubmissionsByOptionalProblemID",
			"problemId": *req.ProblemId,
			"userId":    req.UserId,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, submissionsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache submissions", map[string]any{
			"method":    "GetSubmissionsByOptionalProblemID",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Submissions retrieved successfully", map[string]any{
		"method":    "GetSubmissionsByOptionalProblemID",
		"problemId": *req.ProblemId,
		"userId":    req.UserId,
	}, "SERVICE", nil)
	return resp, nil
}

// GetProblemByIDSlug retrieves a problem by ID or slug
func (s *ProblemService) GetProblemByIDSlug(ctx context.Context, req *pb.GetProblemByIdSlugRequest) (*pb.GetProblemByIdSlugResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetProblemByIDSlug", map[string]any{
		"method":    "GetProblemByIDSlug",
		"problemId": req.ProblemId,
		"slug":      req.Slug,
	}, "SERVICE", nil)

	if req.ProblemId == "" && req.Slug == nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing problem ID or slug", map[string]any{
			"method":    "GetProblemByIDSlug",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID or slug is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
	if req.ProblemId == "" {
		cacheKey = fmt.Sprintf("problem_slug:%s", *req.Slug)
	}

	cachedProblem, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedProblem != nil {
		var problem pb.GetProblemByIdSlugResponse
		cachedStr, ok := cachedProblem.(string)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to assert cached problem to string", map[string]any{
				"method":    "GetProblemByIDSlug",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", nil)
		} else if err := json.Unmarshal([]byte(cachedStr), &problem); err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Problem retrieved from cache", map[string]any{
				"method":    "GetProblemByIDSlug",
				"problemId": req.ProblemId,
				"slug":      req.Slug,
				"cacheKey":  cacheKey,
			}, "SERVICE", nil)
			return &problem, nil
		}
	}

	resp, err := s.RepoConnInstance.GetProblemByIDSlug(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to retrieve problem from DB", map[string]any{
			"method":    "GetProblemByIDSlug",
			"problemId": req.ProblemId,
			"slug":      req.Slug,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	problemBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal problem", map[string]any{
			"method":    "GetProblemByIDSlug",
			"problemId": req.ProblemId,
			"slug":      req.Slug,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, problemBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache problem", map[string]any{
			"method":    "GetProblemByIDSlug",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Problem retrieved successfully", map[string]any{
		"method":    "GetProblemByIDSlug",
		"problemId": req.ProblemId,
		"slug":      req.Slug,
	}, "SERVICE", nil)
	return resp, nil
}

// GetProblemMetadataList retrieves problems by ID list
func (s *ProblemService) GetProblemMetadataList(ctx context.Context, req *pb.GetProblemMetadataListRequest) (*pb.GetProblemMetadataListResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetProblemMetadataList", map[string]any{
		"method":   "GetProblemMetadataList",
		"page":     req.Page,
		"pageSize": req.PageSize,
	}, "SERVICE", nil)

	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = 10
	}

	cacheKey := fmt.Sprintf("problem_id_list:%d:%d", req.Page, req.PageSize)
	cachedProblems, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedProblems != nil {
		var problems pb.GetProblemMetadataListResponse
		cachedStr, ok := cachedProblems.(string)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to assert cached problems to string", map[string]any{
				"method":    "GetProblemMetadataList",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", nil)
		} else if err := json.Unmarshal([]byte(cachedStr), &problems); err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Problem metadata list retrieved from cache", map[string]any{
				"method":   "GetProblemMetadataList",
				"cacheKey": cacheKey,
				"page":     req.Page,
				"pageSize": req.PageSize,
			}, "SERVICE", nil)
			return &problems, nil
		}
	}

	resp, err := s.RepoConnInstance.GetProblemByIDList(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to retrieve problem metadata list from DB", map[string]any{
			"method":    "GetProblemMetadataList",
			"page":      req.Page,
			"pageSize":  req.PageSize,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	problemsBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal problem metadata list", map[string]any{
			"method":    "GetProblemMetadataList",
			"page":      req.Page,
			"pageSize":  req.PageSize,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, problemsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache problem metadata list", map[string]any{
			"method":    "GetProblemMetadataList",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Problem metadata list retrieved successfully", map[string]any{
		"method":   "GetProblemMetadataList",
		"page":     req.Page,
		"pageSize": req.PageSize,
	}, "SERVICE", nil)
	return resp, nil
}

// RunUserCodeProblem executes user code for a problem
func (s *ProblemService) RunUserCodeProblem(ctx context.Context, req *pb.RunProblemRequest) (*pb.RunProblemResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting RunUserCodeProblem", map[string]any{
		"method":        "RunUserCodeProblem",
		"problemId":     req.ProblemId,
		"language":      req.Language,
		"isRunTestcase": req.IsRunTestcase,
	}, "SERVICE", nil)

	problem, err := s.RepoConnInstance.GetProblem(ctx, &pb.GetProblemRequest{ProblemId: req.ProblemId})
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch problem", map[string]any{
			"method":    "RunUserCodeProblem",
			"problemId": req.ProblemId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("problem not found: %w", err)
	}

	submitCase := !req.IsRunTestcase
	validateCode, ok := problem.ValidateCode[req.Language]
	if !ok {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Language not supported", map[string]any{
			"method":    "RunUserCodeProblem",
			"problemId": req.ProblemId,
			"language":  req.Language,
			"errorType": "INVALID_LANGUAGE",
		}, "SERVICE", nil)
		return &pb.RunProblemResponse{
			Success:       false,
			ErrorType:     "INVALID_LANGUAGE",
			Message:       "Language not supported",
			ProblemId:     req.ProblemId,
			Language:      req.Language,
			IsRunTestcase: req.IsRunTestcase,
		}, nil
	}

	var testCases []model.TestCase
	if req.IsRunTestcase {
		for _, tc := range problem.TestCases.Run {
			if tc.ID != "" {
				testCases = append(testCases, model.TestCase{
					ID:       tc.ID,
					Input:    tc.Input,
					Expected: tc.Expected,
				})
			}
		}
	} else {
		for _, tc := range append(problem.TestCases.Run, problem.TestCases.Submit...) {
			if tc.ID != "" {
				testCases = append(testCases, model.TestCase{
					ID:       tc.ID,
					Input:    tc.Input,
					Expected: tc.Expected,
				})
			}
		}
	}

	testCasesJSON, err := json.Marshal(testCases)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal test cases", map[string]any{
			"method":    "RunUserCodeProblem",
			"problemId": req.ProblemId,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to marshal test cases: %w", err)
	}

	tmpl := validateCode.Template
	if req.Language == "python" || req.Language == "javascript" || req.Language == "py" || req.Language == "js" {
		escaped := strings.ReplaceAll(string(testCasesJSON), `"`, `\"`)
		tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", escaped, 1)
	} else {
		tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", string(testCasesJSON), 1)
	}
	tmpl = strings.Replace(tmpl, "{FUNCTION_PLACEHOLDER}", req.UserCode, 1)

	compilerRequest := map[string]any{
		"code":     tmpl,
		"language": req.Language,
	}
	compilerRequestBytes, err := json.Marshal(compilerRequest)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to serialize compiler request", map[string]any{
			"method":    "RunUserCodeProblem",
			"problemId": req.ProblemId,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to serialize compiler request: %w", err)
	}

	msg, err := s.NatsClient.Request("problems.execute.request", compilerRequestBytes, 10*time.Second)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to execute code", map[string]any{
			"method":    "RunUserCodeProblem",
			"problemId": req.ProblemId,
			"errorType": "COMPILATION_ERROR",
		}, "SERVICE", err)
		return &pb.RunProblemResponse{
			Success:       false,
			ErrorType:     "COMPILATION_ERROR",
			Message:       "Failed to execute code",
			ProblemId:     req.ProblemId,
			Language:      req.Language,
			IsRunTestcase: req.IsRunTestcase,
		}, nil
	}

	var result map[string]any
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to parse execution result", map[string]any{
			"method":    "RunUserCodeProblem",
			"problemId": req.ProblemId,
			"errorType": "EXECUTION_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to parse execution result: %w", err)
	}

	output, ok := result["output"].(string)
	if !ok {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Invalid execution result format", map[string]any{
			"method":    "RunUserCodeProblem",
			"problemId": req.ProblemId,
			"errorType": "EXECUTION_ERROR",
		}, "SERVICE", nil)
		return &pb.RunProblemResponse{
			Success:       false,
			ErrorType:     "EXECUTION_ERROR",
			Message:       "Invalid execution result format",
			ProblemId:     req.ProblemId,
			Language:      req.Language,
			IsRunTestcase: req.IsRunTestcase,
		}, nil
	}

	if strings.Contains(output, "syntax error") || strings.Contains(output, "# command-line-arguments") {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Compilation error in user code", map[string]any{
			"method":    "RunUserCodeProblem",
			"problemId": req.ProblemId,
			"errorType": "COMPILATION_ERROR",
		}, "SERVICE", nil)
		go s.processSubmission(ctx, req, "FAILED", submitCase, *problem, req.UserCode)
		return &pb.RunProblemResponse{
			Success:       false,
			ErrorType:     "COMPILATION_ERROR",
			Message:       output,
			ProblemId:     req.ProblemId,
			Language:      req.Language,
			IsRunTestcase: req.IsRunTestcase,
		}, nil
	}

	var executionStatsResult model.ExecutionStatsResult
	if err := json.Unmarshal([]byte(output), &executionStatsResult); err != nil {
		executionStatsResult = model.ExecutionStatsResult{OverallPass: false}
	}

	status := "FAILED"
	if executionStatsResult.OverallPass {
		status = "SUCCESS"
	}

	s.processSubmission(ctx, req, status, submitCase, *problem, req.UserCode)
	if submitCase && req.UserId != "" {
		cacheKeys := []string{
			fmt.Sprintf("submissions:%s:%s", req.ProblemId, req.UserId),
			fmt.Sprintf("stats:%s", req.UserId),
		}
		for _, cacheKey := range cacheKeys {
			if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
				s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
					"method":    "RunUserCodeProblem",
					"cacheKey":  cacheKey,
					"errorType": "CACHE_ERROR",
				}, "SERVICE", err)
			}
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "User code execution completed", map[string]any{
		"method":        "RunUserCodeProblem",
		"problemId":     req.ProblemId,
		"language":      req.Language,
		"isRunTestcase": req.IsRunTestcase,
		"status":        status,
	}, "SERVICE", nil)
	return &pb.RunProblemResponse{
		Success:       true,
		ProblemId:     req.ProblemId,
		Language:      req.Language,
		IsRunTestcase: req.IsRunTestcase,
		Message:       output,
	}, nil
}

// processSubmission handles submission processing
func (s *ProblemService) processSubmission(ctx context.Context, req *pb.RunProblemRequest, status string, submitCasePass bool, problem model.Problem, userCode string) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting processSubmission", map[string]any{
		"method":    "processSubmission",
		"problemId": req.ProblemId,
		"userId":    req.UserId,
		"status":    status,
	}, "SERVICE", nil)

	if !submitCasePass || req.UserId == "" {
		s.logger.Log(zapcore.InfoLevel, traceID, "Skipping submission processing", map[string]any{
			"method":         "processSubmission",
			"problemId":      req.ProblemId,
			"userId":         req.UserId,
			"submitCasePass": submitCasePass,
		}, "SERVICE", nil)
		return
	}

	var submission model.Submission
	if req != nil {
		submission = model.Submission{
			ID:            primitive.NewObjectID(),
			UserID:        req.UserId,
			Country:       *req.Country,
			ProblemID:     req.ProblemId,
			ChallengeID:   nil,
			Title:         problem.Title,
			SubmittedAt:   time.Now(),
			UserCode:      userCode,
			Score:         0,
			Language:      req.Language,
			Status:        status,
			ExecutionTime: 0,
			Difficulty:    problem.Difficulty,
		}
	}

	if err := s.RepoConnInstance.PushSubmissionData(ctx, &submission, status); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to push submission data", map[string]any{
			"method":    "processSubmission",
			"problemId": req.ProblemId,
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
	}

	cacheKeys := []string{
		fmt.Sprintf("submissions:%s:%s", req.ProblemId, req.UserId),
		fmt.Sprintf("heatmap:%s:%d:%d", req.UserId, time.Now().Year(), time.Now().Month()),
		fmt.Sprintf("stats:%s", req.UserId),
	}
	for _, cacheKey := range cacheKeys {
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":    "processSubmission",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Submission processed successfully", map[string]any{
		"method":    "processSubmission",
		"problemId": req.ProblemId,
		"userId":    req.UserId,
		"status":    status,
	}, "SERVICE", nil)
}

// GetProblemsDoneStatistics retrieves problem completion stats
func (s *ProblemService) GetProblemsDoneStatistics(ctx context.Context, req *pb.GetProblemsDoneStatisticsRequest) (*pb.GetProblemsDoneStatisticsResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetProblemsDoneStatistics", map[string]any{
		"method": "GetProblemsDoneStatistics",
		"userId": req.UserId,
	}, "SERVICE", nil)

	cacheKey := fmt.Sprintf("stats:%s", req.UserId)
	cachedStats, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedStats != nil {
		var stats pb.GetProblemsDoneStatisticsResponse
		cachedStr, ok := cachedStats.(string)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to assert cached stats to string", map[string]any{
				"method":    "GetProblemsDoneStatistics",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", nil)
		} else if err := json.Unmarshal([]byte(cachedStr), &stats); err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Problem stats retrieved from cache", map[string]any{
				"method":   "GetProblemsDoneStatistics",
				"userId":   req.UserId,
				"cacheKey": cacheKey,
			}, "SERVICE", nil)
			return &stats, nil
		}
	}

	data, err := s.RepoConnInstance.ProblemsDoneStatistics(req.UserId)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to retrieve problem stats from DB", map[string]any{
			"method":    "GetProblemsDoneStatistics",
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	resp := &pb.GetProblemsDoneStatisticsResponse{
		Data: &pb.ProblemsDoneStatistics{
			MaxEasyCount:    data.MaxEasyCount,
			DoneEasyCount:   data.DoneEasyCount,
			MaxMediumCount:  data.MaxMediumCount,
			DoneMediumCount: data.DoneMediumCount,
			MaxHardCount:    data.MaxHardCount,
			DoneHardCount:   data.DoneHardCount,
		},
	}

	statsBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal problem stats", map[string]any{
			"method":    "GetProblemsDoneStatistics",
			"userId":    req.UserId,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, statsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache problem stats", map[string]any{
			"method":    "GetProblemsDoneStatistics",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Problem stats retrieved successfully", map[string]any{
		"method": "GetProblemsDoneStatistics",
		"userId": req.UserId,
	}, "SERVICE", nil)
	return resp, nil
}

// GetMonthlyActivityHeatmap retrieves monthly activity heatmap
func (s *ProblemService) GetMonthlyActivityHeatmap(ctx context.Context, req *pb.GetMonthlyActivityHeatmapRequest) (*pb.GetMonthlyActivityHeatmapResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetMonthlyActivityHeatmap", map[string]any{
		"method": "GetMonthlyActivityHeatmap",
		"userId": req.UserID,
		"year":   req.Year,
		"month":  req.Month,
	}, "SERVICE", nil)

	cacheKey := fmt.Sprintf("heatmap:%s:%d:%d", req.UserID, req.Year, req.Month)
	cachedData, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedData != nil {
		var heatmap pb.GetMonthlyActivityHeatmapResponse
		cachedStr, ok := cachedData.(string)
		if !ok {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to assert cached heatmap to string", map[string]any{
				"method":    "GetMonthlyActivityHeatmap",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", nil)
		} else if err := json.Unmarshal([]byte(cachedStr), &heatmap); err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Heatmap retrieved from cache", map[string]any{
				"method":   "GetMonthlyActivityHeatmap",
				"userId":   req.UserID,
				"cacheKey": cacheKey,
			}, "SERVICE", nil)
			return &heatmap, nil
		}
	}

	data, err := s.RepoConnInstance.GetMonthlyContributionHistory(req.UserID, int(req.Month), int(req.Year))
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to retrieve heatmap from DB", map[string]any{
			"method":    "GetMonthlyActivityHeatmap",
			"userId":    req.UserID,
			"year":      req.Year,
			"month":     req.Month,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, err
	}

	resp := &pb.GetMonthlyActivityHeatmapResponse{
		Data: make([]*pb.ActivityDay, len(data.Data)),
	}
	for i, day := range data.Data {
		resp.Data[i] = &pb.ActivityDay{
			Date:     day.Date,
			Count:    int32(day.Count),
			IsActive: day.IsActive,
		}
	}

	now := time.Now()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	ttl := time.Until(nextMidnight)

	heatmapBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal heatmap response", map[string]any{
			"method":    "GetMonthlyActivityHeatmap",
			"userId":    req.UserID,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, heatmapBytes, ttl); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache heatmap", map[string]any{
			"method":    "GetMonthlyActivityHeatmap",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Heatmap retrieved successfully", map[string]any{
		"method": "GetMonthlyActivityHeatmap",
		"userId": req.UserID,
		"year":   req.Year,
		"month":  req.Month,
	}, "SERVICE", nil)
	return resp, nil
}

// MapProblemStatistics converts model stats to pb stats
func MapProblemStatistics(input model.ProblemsDoneStatistics) pb.ProblemsDoneStatistics {
	return pb.ProblemsDoneStatistics{
		MaxEasyCount:    input.MaxEasyCount,
		DoneEasyCount:   input.DoneEasyCount,
		MaxMediumCount:  input.MaxMediumCount,
		DoneMediumCount: input.DoneMediumCount,
		MaxHardCount:    input.MaxHardCount,
		DoneHardCount:   input.DoneHardCount,
	}
}

// GetTopKGlobal retrieves top K global users
func (s *ProblemService) GetTopKGlobal(ctx context.Context, req *pb.GetTopKGlobalRequest) (*pb.GetTopKGlobalResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetTopKGlobal", map[string]any{
		"method": "GetTopKGlobal",
		"k":      req.K,
	}, "SERVICE", nil)

	startRedis := time.Now()
	users, err := s.LB.GetTopKGlobal()
	if err == nil && len(users) > 0 {
		s.logger.Log(zapcore.InfoLevel, traceID, "Retrieved top K global users from Redis", map[string]any{
			"method":   "GetTopKGlobal",
			"duration": time.Since(startRedis).String(),
		}, "SERVICE", nil)
		resp := &pb.GetTopKGlobalResponse{
			Users: make([]*pb.UserScore, len(users)),
		}
		for i, user := range users {
			resp.Users[i] = &pb.UserScore{
				UserId: user.ID,
				Score:  user.Score,
				Entity: user.Entity,
			}
		}
		return resp, nil
	}
	s.logger.Log(zapcore.WarnLevel, traceID, "Redis miss for top K global users", map[string]any{
		"method":   "GetTopKGlobal",
		"duration": time.Since(startRedis).String(),
	}, "SERVICE", nil)

	startMongo := time.Now()
	k := int(req.K)
	if k == 0 {
		k = 10
	}
	mongoUsers, err := s.RepoConnInstance.GetTopKGlobalMongo(ctx, k)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch top K global from MongoDB", map[string]any{
			"method":    "GetTopKGlobal",
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to fetch top K global from MongoDB: %w", err)
	}
	s.logger.Log(zapcore.InfoLevel, traceID, "Retrieved top K global users from MongoDB", map[string]any{
		"method":   "GetTopKGlobal",
		"duration": time.Since(startMongo).String(),
	}, "SERVICE", nil)

	resp := &pb.GetTopKGlobalResponse{
		Users: make([]*pb.UserScore, len(mongoUsers)),
	}
	for i, user := range mongoUsers {
		resp.Users[i] = &pb.UserScore{
			UserId: user.ID,
			Score:  user.Score,
			Entity: user.Entity,
		}
	}
	s.logger.Log(zapcore.InfoLevel, traceID, "Top K global users retrieved successfully", map[string]any{
		"method": "GetTopKGlobal",
		"k":      req.K,
	}, "SERVICE", nil)
	return resp, nil
}

// GetTopKEntity retrieves top K users for an entity
func (s *ProblemService) GetTopKEntity(ctx context.Context, req *pb.GetTopKEntityRequest) (*pb.GetTopKEntityResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetTopKEntity", map[string]any{
		"method": "GetTopKEntity",
		"entity": req.Entity,
	}, "SERVICE", nil)

	startRedis := time.Now()
	users, err := s.LB.GetTopKEntity(req.Entity)
	if err == nil && len(users) > 0 {
		s.logger.Log(zapcore.InfoLevel, traceID, "Retrieved top K entity users from Redis", map[string]any{
			"method":   "GetTopKEntity",
			"entity":   req.Entity,
			"duration": time.Since(startRedis).String(),
		}, "SERVICE", nil)
		resp := &pb.GetTopKEntityResponse{
			Users: make([]*pb.UserScore, len(users)),
		}
		for i, user := range users {
			resp.Users[i] = &pb.UserScore{
				UserId: user.ID,
				Score:  user.Score,
				Entity: user.Entity,
			}
		}
		return resp, nil
	}
	s.logger.Log(zapcore.WarnLevel, traceID, "Redis miss for top K entity users", map[string]any{
		"method":   "GetTopKEntity",
		"entity":   req.Entity,
		"duration": time.Since(startRedis).String(),
	}, "SERVICE", nil)

	startMongo := time.Now()
	mongoUsers, err := s.RepoConnInstance.GetTopKEntityMongo(ctx, req.Entity, 10)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch top K entity from MongoDB", map[string]any{
			"method":    "GetTopKEntity",
			"entity":    req.Entity,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to fetch top K entity from MongoDB: %w", err)
	}
	s.logger.Log(zapcore.InfoLevel, traceID, "Retrieved top K entity users from MongoDB", map[string]any{
		"method":   "GetTopKEntity",
		"entity":   req.Entity,
		"duration": time.Since(startMongo).String(),
	}, "SERVICE", nil)

	resp := &pb.GetTopKEntityResponse{
		Users: make([]*pb.UserScore, len(mongoUsers)),
	}
	for i, user := range mongoUsers {
		resp.Users[i] = &pb.UserScore{
			UserId: user.ID,
			Score:  user.Score,
			Entity: user.Entity,
		}
	}
	s.logger.Log(zapcore.InfoLevel, traceID, "Top K entity users retrieved successfully", map[string]any{
		"method": "GetTopKEntity",
		"entity": req.Entity,
	}, "SERVICE", nil)
	return resp, nil
}

// GetUserRank retrieves a user's rank
func (s *ProblemService) GetUserRank(ctx context.Context, req *pb.GetUserRankRequest) (*pb.GetUserRankResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetUserRank", map[string]any{
		"method": "GetUserRank",
		"userId": req.UserId,
	}, "SERVICE", nil)

	startRedis := time.Now()
	globalRank, err := s.LB.GetRankGlobal(req.UserId)
	if err == nil {
		entityRank, err := s.LB.GetRankEntity(req.UserId)
		if err == nil {
			s.logger.Log(zapcore.InfoLevel, traceID, "Retrieved user rank from Redis", map[string]any{
				"method":     "GetUserRank",
				"userId":     req.UserId,
				"globalRank": globalRank,
				"entityRank": entityRank,
				"duration":   time.Since(startRedis).String(),
			}, "SERVICE", nil)
			return &pb.GetUserRankResponse{
				GlobalRank: int32(globalRank),
				EntityRank: int32(entityRank),
			}, nil
		}
	}
	s.logger.Log(zapcore.WarnLevel, traceID, "Redis miss for user rank", map[string]any{
		"method":   "GetUserRank",
		"userId":   req.UserId,
		"duration": time.Since(startRedis).String(),
	}, "SERVICE", nil)

	startMongo := time.Now()
	globalRank, entityRank, err := s.RepoConnInstance.GetUserRankMongo(ctx, req.UserId)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch user rank from MongoDB", map[string]any{
			"method":    "GetUserRank",
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to fetch user rank from MongoDB: %w", err)
	}
	s.logger.Log(zapcore.InfoLevel, traceID, "Retrieved user rank from MongoDB", map[string]any{
		"method":     "GetUserRank",
		"userId":     req.UserId,
		"globalRank": globalRank,
		"entityRank": entityRank,
		"duration":   time.Since(startMongo).String(),
	}, "SERVICE", nil)

	s.logger.Log(zapcore.InfoLevel, traceID, "User rank retrieved successfully", map[string]any{
		"method":     "GetUserRank",
		"userId":     req.UserId,
		"globalRank": globalRank,
		"entityRank": entityRank,
	}, "SERVICE", nil)
	return &pb.GetUserRankResponse{
		GlobalRank: int32(globalRank),
		EntityRank: int32(entityRank),
	}, nil
}

// GetLeaderboardData retrieves leaderboard data for a user
func (s *ProblemService) GetLeaderboardData(ctx context.Context, req *pb.GetLeaderboardDataRequest) (*pb.GetLeaderboardDataResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetLeaderboardData", map[string]any{
		"method": "GetLeaderboardData",
		"userId": req.UserId,
	}, "SERVICE", nil)

	startRedis := time.Now()
	data, err := s.LB.GetUserLeaderboardData(req.UserId)
	if err == nil {
		s.logger.Log(zapcore.InfoLevel, traceID, "Retrieved leaderboard data from Redis", map[string]any{
			"method":   "GetLeaderboardData",
			"userId":   req.UserId,
			"duration": time.Since(startRedis).String(),
		}, "SERVICE", nil)
		resp := &pb.GetLeaderboardDataResponse{
			UserId:     data.UserID,
			Score:      data.Score,
			Entity:     data.Entity,
			GlobalRank: int32(data.GlobalRank),
			EntityRank: int32(data.EntityRank),
			TopKGlobal: make([]*pb.UserScore, len(data.TopKGlobal)),
			TopKEntity: make([]*pb.UserScore, len(data.TopKEntity)),
		}
		for i, user := range data.TopKGlobal {
			resp.TopKGlobal[i] = &pb.UserScore{
				UserId: user.ID,
				Score:  user.Score,
				Entity: user.Entity,
			}
		}
		for i, user := range data.TopKEntity {
			resp.TopKEntity[i] = &pb.UserScore{
				UserId: user.ID,
				Score:  user.Score,
				Entity: user.Entity,
			}
		}
		return resp, nil
	}
	s.logger.Log(zapcore.WarnLevel, traceID, "Redis miss for leaderboard data", map[string]any{
		"method":   "GetLeaderboardData",
		"userId":   req.UserId,
		"duration": time.Since(startRedis).String(),
	}, "SERVICE", nil)

	startMongo := time.Now()

	mongoData, err := s.RepoConnInstance.GetLeaderboardDataMongo(ctx, req.UserId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			s.logger.Log(zapcore.ErrorLevel, traceID, "User not found in MongoDB", map[string]any{
				"method":    "GetLeaderboardData",
				"userId":    req.UserId,
				"errorType": "NOT_FOUND",
			}, "SERVICE", err)
			return nil, errors.New("user not found")
		}
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch leaderboard data from MongoDB", map[string]any{
			"method":    "GetLeaderboardData",
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to fetch leaderboard data from MongoDB: %w", err)
	}

	globalRank, entityRank, err := s.RepoConnInstance.GetUserRankMongo(ctx, req.UserId)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch user ranks from MongoDB", map[string]any{
			"method":    "GetLeaderboardData",
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to fetch user ranks from MongoDB: %w", err)
	}
	topKGlobal, err := s.RepoConnInstance.GetTopKGlobalMongo(ctx, 10)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch top K global from MongoDB", map[string]any{
			"method":    "GetLeaderboardData",
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, fmt.Errorf("failed to fetch top K global from MongoDB: %w", err)
	}
	var topKEntity []model.UserScore
	if mongoData.Entity != "" {
		topKEntity, err = s.RepoConnInstance.GetTopKEntityMongo(ctx, mongoData.Entity, 10)
		if err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch top K entity from MongoDB", map[string]any{
				"method":    "GetLeaderboardData",
				"userId":    req.UserId,
				"entity":    mongoData.Entity,
				"errorType": "DB_ERROR",
			}, "SERVICE", err)
			return nil, fmt.Errorf("failed to fetch top K entity from MongoDB: %w", err)
		}
	}
	s.logger.Log(zapcore.InfoLevel, traceID, "Retrieved leaderboard data from MongoDB", map[string]any{
		"method":   "GetLeaderboardData",
		"userId":   req.UserId,
		"duration": time.Since(startMongo).String(),
	}, "SERVICE", nil)

	resp := &pb.GetLeaderboardDataResponse{
		UserId:     mongoData.ID,
		Score:      mongoData.Score,
		Entity:     mongoData.Entity,
		GlobalRank: int32(globalRank),
		EntityRank: int32(entityRank),
		TopKGlobal: make([]*pb.UserScore, len(topKGlobal)),
		TopKEntity: make([]*pb.UserScore, len(topKEntity)),
	}
	for i, user := range topKGlobal {
		resp.TopKGlobal[i] = &pb.UserScore{
			UserId: user.ID,
			Score:  user.Score,
			Entity: user.Entity,
		}
	}
	for i, user := range topKEntity {
		resp.TopKEntity[i] = &pb.UserScore{
			UserId: user.ID,
			Score:  user.Score,
			Entity: user.Entity,
		}
	}
	s.logger.Log(zapcore.InfoLevel, traceID, "Leaderboard data retrieved successfully", map[string]any{
		"method": "GetLeaderboardData",
		"userId": req.UserId,
	}, "SERVICE", nil)
	return resp, nil
}

// CreateChallenge creates a new challenge
func (s *ProblemService) CreateChallenge(ctx context.Context, req *pb.CreateChallengeRequest) (*pb.CreateChallengeResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting CreateChallenge", map[string]any{
		"method":     "CreateChallenge",
		"title":      req.Title,
		"creatorId":  req.CreatorId,
		"difficulty": req.Difficulty,
	}, "SERVICE", nil)

	if req.Title == "" || req.CreatorId == "" || req.Difficulty == "" || len(req.ProblemIds) == 0 {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing required fields", map[string]any{
			"method":    "CreateChallenge",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "title, creator_id, difficulty, and at least one problem_id are required", "VALIDATION_ERROR", nil)
	}

	roomCode, err := generateAccessCode(8)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to generate room code", map[string]any{
			"method":    "CreateChallenge",
			"errorType": "INTERNAL_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to generate room code", "INTERNAL_ERROR", err)
	}

	password := ""
	if req.IsPrivate {
		pass, err := generateAccessCode(12)
		if err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to generate password", map[string]any{
				"method":    "CreateChallenge",
				"errorType": "INTERNAL_ERROR",
			}, "SERVICE", err)
			return nil, s.createGrpcError(codes.Internal, "failed to generate password", "INTERNAL_ERROR", err)
		}
		password = pass
	}

	resp, err := s.RepoConnInstance.CreateChallenge(ctx, req, roomCode, password)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to create challenge", map[string]any{
			"method":    "CreateChallenge",
			"title":     req.Title,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to create challenge", "DB_ERROR", err)
	}

	if !req.IsPrivate {
		cacheKey := "challenges:public:*"
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":    "CreateChallenge",
				"cacheKey":  cacheKey,
				"errorType": "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Challenge created successfully", map[string]any{
		"method":      "CreateChallenge",
		"title":       req.Title,
		"challengeId": resp.Id,
	}, "SERVICE", nil)
	return &pb.CreateChallengeResponse{
		Id:       resp.Id,
		Password: password,
		JoinUrl:  fmt.Sprintf("https://xcode.com/challenges/join/%s/%v", resp.Id, resp.Password),
	}, nil
}

// GetChallengeDetails retrieves challenge details
func (s *ProblemService) GetChallengeDetails(ctx context.Context, req *pb.GetChallengeDetailsRequest) (*pb.GetChallengeDetailsResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetChallengeDetails", map[string]any{
		"method":      "GetChallengeDetails",
		"challengeId": req.Id,
	}, "SERVICE", nil)

	if req.Id == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing challenge ID", map[string]any{
			"method":    "GetChallengeDetails",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id and user_id are required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("challenge_details:%s", req.Id)
	cachedDetails, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedDetails != nil {
		var details pb.GetChallengeDetailsResponse
		if cachedStr, ok := cachedDetails.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &details); err == nil {
				s.logger.Log(zapcore.InfoLevel, traceID, "Challenge details retrieved from cache", map[string]any{
					"method":      "GetChallengeDetails",
					"challengeId": req.Id,
					"cacheKey":    cacheKey,
				}, "SERVICE", nil)
				return &details, nil
			}
		}
	}

	challengeResp, err := s.RepoConnInstance.GetChallenge(ctx, &pb.GetChallengeDetailsRequest{Id: req.Id})
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch challenge from DB", map[string]any{
			"method":      "GetChallengeDetails",
			"challengeId": req.Id,
			"errorType":   "NOT_FOUND",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.NotFound, "challenge not found", "NOT_FOUND", err)
	}

	leaderboard, err := s.RepoConnInstance.GetChallengeLeaderboard(ctx, req.Id)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch leaderboard", map[string]any{
			"method":      "GetChallengeDetails",
			"challengeId": req.Id,
			"errorType":   "DB_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to fetch leaderboard", "DB_ERROR", err)
	}

	response := &pb.GetChallengeDetailsResponse{
		Challenge:   challengeResp.Challenge,
		Leaderboard: leaderboard,
	}

	detailsBytes, err := json.Marshal(response)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal challenge details", map[string]any{
			"method":      "GetChallengeDetails",
			"challengeId": req.Id,
			"errorType":   "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, detailsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache challenge details", map[string]any{
			"method":      "GetChallengeDetails",
			"challengeId": req.Id,
			"cacheKey":    cacheKey,
			"errorType":   "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Challenge details retrieved successfully", map[string]any{
		"method":      "GetChallengeDetails",
		"challengeId": req.Id,
	}, "SERVICE", nil)
	return response, nil
}

// GetPublicChallenges retrieves public challenges
func (s *ProblemService) GetPublicChallenges(ctx context.Context, req *pb.GetPublicChallengesRequest) (*pb.GetPublicChallengesResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetPublicChallenges", map[string]any{
		"method":     "GetPublicChallenges",
		"page":       req.Page,
		"pageSize":   req.PageSize,
		"difficulty": req.Difficulty,
		"isActive":   req.IsActive,
		"userId":     req.UserId,
	}, "SERVICE", nil)

	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = 10
	}

	cacheKey := fmt.Sprintf("challenges:public:%s:%v:%v", req.Difficulty, req.IsActive, req.UserId)
	cachedChallenges, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedChallenges != nil {
		var challenges pb.GetPublicChallengesResponse
		if cachedStr, ok := cachedChallenges.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &challenges); err == nil {
				s.logger.Log(zapcore.InfoLevel, traceID, "Public challenges retrieved from cache", map[string]any{
					"method":   "GetPublicChallenges",
					"cacheKey": cacheKey,
				}, "SERVICE", nil)
				return &challenges, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetPublicChallenges(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch public challenges from DB", map[string]any{
			"method":    "GetPublicChallenges",
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to fetch public challenges", "DB_ERROR", err)
	}

	challengesBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal public challenges", map[string]any{
			"method":    "GetPublicChallenges",
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, challengesBytes, 1*time.Minute); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache public challenges", map[string]any{
			"method":    "GetPublicChallenges",
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Public challenges retrieved successfully", map[string]any{
		"method":   "GetPublicChallenges",
		"page":     req.Page,
		"pageSize": req.PageSize,
	}, "SERVICE", nil)
	return resp, nil
}

// JoinChallenge allows a user to join a challenge
func (s *ProblemService) JoinChallenge(ctx context.Context, req *pb.JoinChallengeRequest) (*pb.JoinChallengeResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting JoinChallenge", map[string]any{
		"method":      "JoinChallenge",
		"challengeId": req.ChallengeId,
		"userId":      req.UserId,
	}, "SERVICE", nil)

	if req.ChallengeId == "" || req.UserId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing challenge ID or user ID", map[string]any{
			"method":    "JoinChallenge",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id, user_id are required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.JoinChallenge(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to join challenge", map[string]any{
			"method":      "JoinChallenge",
			"challengeId": req.ChallengeId,
			"userId":      req.UserId,
			"errorType":   "ACCESS_DENIED",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.InvalidArgument, "failed to join challenge", "ACCESS_DENIED", err)
	}

	cacheKey := fmt.Sprintf("challenge_details:%s:*", req.ChallengeId)
	if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
			"method":      "JoinChallenge",
			"challengeId": req.ChallengeId,
			"cacheKey":    cacheKey,
			"errorType":   "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Challenge joined successfully", map[string]any{
		"method":      "JoinChallenge",
		"challengeId": req.ChallengeId,
		"userId":      req.UserId,
	}, "SERVICE", nil)
	return resp, nil
}

// StartChallenge starts a challenge
func (s *ProblemService) StartChallenge(ctx context.Context, req *pb.StartChallengeRequest) (*pb.StartChallengeResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting StartChallenge", map[string]any{
		"method":      "StartChallenge",
		"challengeId": req.ChallengeId,
		"userId":      req.UserId,
	}, "SERVICE", nil)

	if req.ChallengeId == "" || req.UserId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing challenge ID or user ID", map[string]any{
			"method":    "StartChallenge",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id and user_id are required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.StartChallenge(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to start challenge", map[string]any{
			"method":      "StartChallenge",
			"challengeId": req.ChallengeId,
			"userId":      req.UserId,
			"errorType":   "INVALID_STATE",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.InvalidArgument, "failed to start challenge", "INVALID_STATE", err)
	}

	cacheKeys := []string{
		fmt.Sprintf("challenge_details:%s:*", req.ChallengeId),
		"challenges:public:*",
	}
	for _, cacheKey := range cacheKeys {
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":      "StartChallenge",
				"challengeId": req.ChallengeId,
				"cacheKey":    cacheKey,
				"errorType":   "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Challenge started successfully", map[string]any{
		"method":      "StartChallenge",
		"challengeId": req.ChallengeId,
		"userId":      req.UserId,
	}, "SERVICE", nil)
	return resp, nil
}

// EndChallenge ends a challenge
func (s *ProblemService) EndChallenge(ctx context.Context, req *pb.EndChallengeRequest) (*pb.EndChallengeResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting EndChallenge", map[string]any{
		"method":      "EndChallenge",
		"challengeId": req.ChallengeId,
		"userId":      req.UserId,
	}, "SERVICE", nil)

	if req.ChallengeId == "" || req.UserId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing challenge ID or user ID", map[string]any{
			"method":    "EndChallenge",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id and user_id are required", "VALIDATION_ERROR", nil)
	}

	leaderboard, err := s.RepoConnInstance.GetChallengeLeaderboard(ctx, req.ChallengeId)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch leaderboard", map[string]any{
			"method":      "EndChallenge",
			"challengeId": req.ChallengeId,
			"errorType":   "DB_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to fetch leaderboard", "DB_ERROR", err)
	}

	resp, err := s.RepoConnInstance.EndChallenge(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to end challenge", map[string]any{
			"method":      "EndChallenge",
			"challengeId": req.ChallengeId,
			"userId":      req.UserId,
			"errorType":   "INVALID_STATE",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.InvalidArgument, "failed to end challenge", "INVALID_STATE", err)
	}

	cacheKeys := []string{
		fmt.Sprintf("challenge_details:%s:*", req.ChallengeId),
		"challenges:public:*",
	}
	for _, cacheKey := range cacheKeys {
		if err := s.RedisCacheClient.Delete(cacheKey); err != nil {
			s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to delete cache", map[string]any{
				"method":      "EndChallenge",
				"challengeId": req.ChallengeId,
				"cacheKey":    cacheKey,
				"errorType":   "CACHE_ERROR",
			}, "SERVICE", err)
		}
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Challenge ended successfully", map[string]any{
		"method":      "EndChallenge",
		"challengeId": req.ChallengeId,
		"userId":      req.UserId,
	}, "SERVICE", nil)
	return &pb.EndChallengeResponse{
		Success:     resp.Success,
		Leaderboard: leaderboard,
	}, nil
}

// GetSubmissionStatus retrieves submission status
func (s *ProblemService) GetSubmissionStatus(ctx context.Context, req *pb.GetSubmissionStatusRequest) (*pb.GetSubmissionStatusResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetSubmissionStatus", map[string]any{
		"method":       "GetSubmissionStatus",
		"submissionId": req.SubmissionId,
	}, "SERVICE", nil)

	if req.SubmissionId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing submission ID", map[string]any{
			"method":    "GetSubmissionStatus",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "submission_id is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("submission:%s", req.SubmissionId)
	cachedSubmission, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedSubmission != nil {
		var submission pb.GetSubmissionStatusResponse
		if cachedStr, ok := cachedSubmission.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &submission); err == nil {
				s.logger.Log(zapcore.InfoLevel, traceID, "Submission status retrieved from cache", map[string]any{
					"method":       "GetSubmissionStatus",
					"submissionId": req.SubmissionId,
					"cacheKey":     cacheKey,
				}, "SERVICE", nil)
				return &submission, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetSubmissionStatus(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch submission from DB", map[string]any{
			"method":       "GetSubmissionStatus",
			"submissionId": req.SubmissionId,
			"errorType":    "NOT_FOUND",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.NotFound, "submission not found", "NOT_FOUND", err)
	}

	submissionBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal submission", map[string]any{
			"method":       "GetSubmissionStatus",
			"submissionId": req.SubmissionId,
			"errorType":    "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, submissionBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache submission", map[string]any{
			"method":       "GetSubmissionStatus",
			"submissionId": req.SubmissionId,
			"cacheKey":     cacheKey,
			"errorType":    "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Submission status retrieved successfully", map[string]any{
		"method":       "GetSubmissionStatus",
		"submissionId": req.SubmissionId,
	}, "SERVICE", nil)
	return resp, nil
}

// GetChallengeSubmissions retrieves challenge submissions
func (s *ProblemService) GetChallengeSubmissions(ctx context.Context, req *pb.GetChallengeSubmissionsRequest) (*pb.GetChallengeSubmissionsResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetChallengeSubmissions", map[string]any{
		"method":      "GetChallengeSubmissions",
		"challengeId": req.ChallengeId,
	}, "SERVICE", nil)

	if req.ChallengeId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing challenge ID", map[string]any{
			"method":    "GetChallengeSubmissions",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("challenge_submissions:%s", req.ChallengeId)
	cachedSubmissions, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedSubmissions != nil {
		var submissions pb.GetChallengeSubmissionsResponse
		if cachedStr, ok := cachedSubmissions.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &submissions); err == nil {
				s.logger.Log(zapcore.InfoLevel, traceID, "Challenge submissions retrieved from cache", map[string]any{
					"method":      "GetChallengeSubmissions",
					"challengeId": req.ChallengeId,
					"cacheKey":    cacheKey,
				}, "SERVICE", nil)
				return &submissions, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetChallengeSubmissions(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch challenge submissions from DB", map[string]any{
			"method":      "GetChallengeSubmissions",
			"challengeId": req.ChallengeId,
			"errorType":   "DB_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to fetch submissions", "DB_ERROR", err)
	}

	submissionsBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal challenge submissions", map[string]any{
			"method":      "GetChallengeSubmissions",
			"challengeId": req.ChallengeId,
			"errorType":   "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, submissionsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache challenge submissions", map[string]any{
			"method":      "GetChallengeSubmissions",
			"challengeId": req.ChallengeId,
			"cacheKey":    cacheKey,
			"errorType":   "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Challenge submissions retrieved successfully", map[string]any{
		"method":      "GetChallengeSubmissions",
		"challengeId": req.ChallengeId,
	}, "SERVICE", nil)
	return resp, nil
}

// GetUserStats retrieves user statistics
func (s *ProblemService) GetUserStats(ctx context.Context, req *pb.GetUserStatsRequest) (*pb.GetUserStatsResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetUserStats", map[string]any{
		"method": "GetUserStats",
		"userId": req.UserId,
	}, "SERVICE", nil)

	if req.UserId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing user ID", map[string]any{
			"method":    "GetUserStats",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "user_id is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("user_stats:%s", req.UserId)
	cachedStats, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedStats != nil {
		var stats pb.GetUserStatsResponse
		if cachedStr, ok := cachedStats.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &stats); err == nil {
				s.logger.Log(zapcore.InfoLevel, traceID, "User stats retrieved from cache", map[string]any{
					"method":   "GetUserStats",
					"userId":   req.UserId,
					"cacheKey": cacheKey,
				}, "SERVICE", nil)
				return &stats, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetUserStats(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch user stats from DB", map[string]any{
			"method":    "GetUserStats",
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to fetch user stats", "DB_ERROR", err)
	}

	statsBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal user stats", map[string]any{
			"method":    "GetUserStats",
			"userId":    req.UserId,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, statsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache user stats", map[string]any{
			"method":    "GetUserStats",
			"userId":    req.UserId,
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "User stats retrieved successfully", map[string]any{
		"method": "GetUserStats",
		"userId": req.UserId,
	}, "SERVICE", nil)
	return resp, nil
}

// GetChallengeUserStats retrieves user statistics for a challenge
func (s *ProblemService) GetChallengeUserStats(ctx context.Context, req *pb.GetChallengeUserStatsRequest) (*pb.GetChallengeUserStatsResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetChallengeUserStats", map[string]any{
		"method":      "GetChallengeUserStats",
		"challengeId": req.ChallengeId,
		"userId":      req.UserId,
	}, "SERVICE", nil)

	if req.ChallengeId == "" || req.UserId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing challenge ID or user ID", map[string]any{
			"method":    "GetChallengeUserStats",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id and user_id are required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("challenge_user_stats:%s:%s", req.ChallengeId, req.UserId)
	cachedStats, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedStats != nil {
		var stats pb.GetChallengeUserStatsResponse
		if cachedStr, ok := cachedStats.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &stats); err == nil {
				s.logger.Log(zapcore.InfoLevel, traceID, "Challenge user stats retrieved from cache", map[string]any{
					"method":      "GetChallengeUserStats",
					"challengeId": req.ChallengeId,
					"userId":      req.UserId,
					"cacheKey":    cacheKey,
				}, "SERVICE", nil)
				return &stats, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetChallengeUserStats(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch challenge user stats from DB", map[string]any{
			"method":      "GetChallengeUserStats",
			"challengeId": req.ChallengeId,
			"userId":      req.UserId,
			"errorType":   "DB_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to fetch challenge user stats", "DB_ERROR", err)
	}

	statsBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal challenge user stats", map[string]any{
			"method":      "GetChallengeUserStats",
			"challengeId": req.ChallengeId,
			"userId":      req.UserId,
			"errorType":   "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, statsBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache challenge user stats", map[string]any{
			"method":      "GetChallengeUserStats",
			"challengeId": req.ChallengeId,
			"userId":      req.UserId,
			"cacheKey":    cacheKey,
			"errorType":   "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Challenge user stats retrieved successfully", map[string]any{
		"method":      "GetChallengeUserStats",
		"challengeId": req.ChallengeId,
		"userId":      req.UserId,
	}, "SERVICE", nil)
	return resp, nil
}

// generateAccessCode generates a random access code
func generateAccessCode(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	for i, v := range b {
		b[i] = charset[v%byte(len(charset))]
	}
	return string(b), nil
}

// GetChallengeHistory retrieves a paginated list of challenges a user has participated in
func (s *ProblemService) GetChallengeHistory(ctx context.Context, req *pb.GetChallengeHistoryRequest) (*pb.GetChallengeHistoryResponse, error) {
	traceID := uuid.New().String()
	s.logger.Log(zapcore.InfoLevel, traceID, "Starting GetChallengeHistory", map[string]any{
		"method":   "GetChallengeHistory",
		"userId":   req.UserId,
		"page":     req.Page,
		"pageSize": req.PageSize,
	}, "SERVICE", nil)

	if req.UserId == "" {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Missing user ID", map[string]any{
			"method":    "GetChallengeHistory",
			"errorType": "VALIDATION_ERROR",
		}, "SERVICE", nil)
		return nil, s.createGrpcError(codes.InvalidArgument, "user_id is required", "VALIDATION_ERROR", nil)
	}

	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = 10
	}

	cacheKey := fmt.Sprintf("user_challenge_history:%s:%d:%d", req.UserId, req.Page, req.PageSize)
	cachedChallenges, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedChallenges != nil {
		var challenges pb.GetChallengeHistoryResponse
		if cachedStr, ok := cachedChallenges.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &challenges); err == nil {
				s.logger.Log(zapcore.InfoLevel, traceID, "Challenge history retrieved from cache", map[string]any{
					"method":   "GetChallengeHistory",
					"userId":   req.UserId,
					"cacheKey": cacheKey,
				}, "SERVICE", nil)
				return &challenges, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetChallengeHistory(ctx, req)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to fetch challenge history from DB", map[string]any{
			"method":    "GetChallengeHistory",
			"userId":    req.UserId,
			"errorType": "DB_ERROR",
		}, "SERVICE", err)
		return nil, s.createGrpcError(codes.Internal, "failed to fetch challenge history", "DB_ERROR", err)
	}

	challengesBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to marshal challenge history", map[string]any{
			"method":    "GetChallengeHistory",
			"userId":    req.UserId,
			"errorType": "MARSHAL_ERROR",
		}, "SERVICE", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, challengesBytes, 5*time.Second); err != nil {
		s.logger.Log(zapcore.ErrorLevel, traceID, "Failed to cache challenge history", map[string]any{
			"method":    "GetChallengeHistory",
			"userId":    req.UserId,
			"cacheKey":  cacheKey,
			"errorType": "CACHE_ERROR",
		}, "SERVICE", err)
	}

	s.logger.Log(zapcore.InfoLevel, traceID, "Challenge history retrieved successfully", map[string]any{
		"method":   "GetChallengeHistory",
		"userId":   req.UserId,
		"page":     req.Page,
		"pageSize": req.PageSize,
	}, "SERVICE", nil)
	return resp, nil
}

func (s *ProblemService) ForceChangeUserEntityInSubmission(ctx context.Context, req *pb.ForceChangeUserEntityInSubmissionRequest) (*pb.ForceChangeUserEntityInSubmissionResponse, error) {
	s.LB.UpdateEntityByUserID(req.UserId, req.Entity)
	s.RepoConnInstance.ForceChangeUserCountryInSubmission(ctx, req)
	return &pb.ForceChangeUserEntityInSubmissionResponse{}, nil
}
