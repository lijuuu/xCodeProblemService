package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"xcode/cache"
	"xcode/model"
	"xcode/natsclient"
	"xcode/repository"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	redisboard "github.com/lijuuu/RedisBoard"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zap_betterstack "xcode/logger"
)

// problemservice handles problem-related operations
type ProblemService struct {
	RepoConnInstance repository.Repository
	NatsClient       *natsclient.NatsClient
	RedisCacheClient cache.RedisCache
	LB               *redisboard.Leaderboard
	pb.UnimplementedProblemsServiceServer
	logger *zap_betterstack.BetterStackLogStreamer
	
}

func NewService(repo repository.Repository, natsClient *natsclient.NatsClient, redisCache cache.RedisCache, lb *redisboard.Leaderboard,logger *zap_betterstack.BetterStackLogStreamer) *ProblemService {
	svc := &ProblemService{
		RepoConnInstance: repo,
		NatsClient:       natsClient,
		RedisCacheClient: redisCache,
		LB:               lb,
		
		logger: logger,
	}
	// Sync leaderboard during initialization
	if err := svc.SyncLeaderboardFromMongo(context.Background()); err != nil {
		log.Fatalf("Failed to sync leaderboard during service initialization: %v", err)
	}
	return svc
}

func (s *ProblemService) SyncLeaderboardFromMongo(ctx context.Context) error {
	return s.RepoConnInstance.SyncLeaderboardToRedis(ctx)
}

// getservice returns the problemservice instance
func (s *ProblemService) GetService() *ProblemService {
	return s
}

// creategrpcerror constructs a grpc error
func (s *ProblemService) createGrpcError(code codes.Code, message string, errorType string, cause error) error {
	details := message
	if cause != nil {
		details = cause.Error()
	}
	return status.Error(code, fmt.Sprintf("ErrorType: %s, Code: %d, Details: %s", errorType, code, details))
}

// createproblem creates a new problem
func (s *ProblemService) CreateProblem(ctx context.Context, req *pb.CreateProblemRequest) (*pb.CreateProblemResponse, error) {
	if req.Title == "" || req.Description == "" || req.Difficulty == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Title, description, and difficulty are required", "VALIDATION_ERROR", nil)
	}
	resp, err := s.RepoConnInstance.CreateProblem(ctx, req)
	if err == nil {
		cacheKey := "problems_list:*"
		_ = s.RedisCacheClient.Delete(cacheKey)
	}
	return resp, err
}

// updateproblem updates an existing problem
func (s *ProblemService) UpdateProblem(ctx context.Context, req *pb.UpdateProblemRequest) (*pb.UpdateProblemResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	resp, err := s.RepoConnInstance.UpdateProblem(ctx, req)
	if err == nil {
		cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
		cacheKey = fmt.Sprintf("problem_slug:%s", *req.Title)
		_ = s.RedisCacheClient.Delete(cacheKey)
		cacheKey = "problems_list:*"
		_ = s.RedisCacheClient.Delete(cacheKey)
	}
	return resp, err
}

// deleteproblem deletes a problem
func (s *ProblemService) DeleteProblem(ctx context.Context, req *pb.DeleteProblemRequest) (*pb.DeleteProblemResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	resp, err := s.RepoConnInstance.DeleteProblem(ctx, req)
	if err == nil {
		cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
		cacheKey = "problems_list:*"
		_ = s.RedisCacheClient.Delete(cacheKey)
	}
	return resp, err
}

// getproblem retrieves a problem by id
func (s *ProblemService) GetProblem(ctx context.Context, req *pb.GetProblemRequest) (*pb.GetProblemResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
	cachedProblem, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedProblem != nil {
		var problem pb.GetProblemResponse
		cachedStr, ok := cachedProblem.(string)
		if !ok {
			fmt.Println("failed to assert cached problem to string")
			goto fetchFromDB
		}
		if err := json.Unmarshal([]byte(cachedStr), &problem); err == nil {
			return &problem, nil
		}
	}
fetchFromDB:
	problemRepoModel, err := s.RepoConnInstance.GetProblem(ctx, req)
	if err != nil {
		return nil, err
	}
	problemPB := repository.ToProblemResponse(*problemRepoModel)
	problemBytes, _ := json.Marshal(problemPB)
	if err := s.RedisCacheClient.Set(cacheKey, problemBytes, 1*time.Hour); err != nil {
		fmt.Printf("failed to cache problem: %v\n", err)
	}
	return problemPB, nil
}

// listproblems retrieves a paginated list of problems
func (s *ProblemService) ListProblems(ctx context.Context, req *pb.ListProblemsRequest) (*pb.ListProblemsResponse, error) {
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
			fmt.Println("failed to assert cached problems to string")
			goto fetchFromDB
		}
		if err := json.Unmarshal([]byte(cachedStr), &problems); err == nil {
			return &problems, nil
		}
	}
fetchFromDB:
	resp, err := s.RepoConnInstance.ListProblems(ctx, req)
	if err != nil {
		return nil, err
	}
	problemsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, problemsBytes, 1*time.Hour); err != nil {
		fmt.Printf("failed to cache problems list: %v\n", err)
	}
	return resp, nil
}

// addtestcases adds test cases to a problem
func (s *ProblemService) AddTestCases(ctx context.Context, req *pb.AddTestCasesRequest) (*pb.AddTestCasesResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	if len(req.Testcases.Run) == 0 && len(req.Testcases.Submit) == 0 {
		return nil, s.createGrpcError(codes.InvalidArgument, "At least one test case is required", "VALIDATION_ERROR", nil)
	}
	for _, tc := range req.Testcases.Run {
		if tc.Input == "" || tc.Expected == "" {
			return nil, s.createGrpcError(codes.InvalidArgument, "Test case input and expected output are required", "VALIDATION_ERROR", nil)
		}
	}
	for _, tc := range req.Testcases.Submit {
		if tc.Input == "" || tc.Expected == "" {
			return nil, s.createGrpcError(codes.InvalidArgument, "Test case input and expected output are required", "VALIDATION_ERROR", nil)
		}
	}
	resp, err := s.RepoConnInstance.AddTestCases(ctx, req)
	if err == nil {
		cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
	}
	return resp, err
}

// addlanguagesupport adds language support to a problem
func (s *ProblemService) AddLanguageSupport(ctx context.Context, req *pb.AddLanguageSupportRequest) (*pb.AddLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}
	if req.ValidationCode == nil || req.ValidationCode.Code == "" || req.ValidationCode.Template == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Validation code (code and template) is required", "VALIDATION_ERROR", nil)
	}
	resp, err := s.RepoConnInstance.AddLanguageSupport(ctx, req)
	if err == nil {
		cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
		cacheKey = fmt.Sprintf("language_supports:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
	}
	return resp, err
}

// updatelanguagesupport updates language support for a problem
func (s *ProblemService) UpdateLanguageSupport(ctx context.Context, req *pb.UpdateLanguageSupportRequest) (*pb.UpdateLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}
	if req.ValidationCode == nil || req.ValidationCode.Code == "" || req.ValidationCode.Template == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Validation code (code and template) is required", "VALIDATION_ERROR", nil)
	}
	resp, err := s.RepoConnInstance.UpdateLanguageSupport(ctx, req)
	if err == nil {
		cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
		cacheKey = fmt.Sprintf("language_supports:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
	}
	return resp, err
}

// removelanguagesupport removes language support from a problem
func (s *ProblemService) RemoveLanguageSupport(ctx context.Context, req *pb.RemoveLanguageSupportRequest) (*pb.RemoveLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}
	resp, err := s.RepoConnInstance.RemoveLanguageSupport(ctx, req)
	if err == nil {
		cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
		cacheKey = fmt.Sprintf("language_supports:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
	}
	return resp, err
}

// deletetestcase deletes a test case from a problem
func (s *ProblemService) DeleteTestCase(ctx context.Context, req *pb.DeleteTestCaseRequest) (*pb.DeleteTestCaseResponse, error) {
	if req.ProblemId == "" || req.TestcaseId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and testcase ID are required", "VALIDATION_ERROR", nil)
	}
	resp, err := s.RepoConnInstance.DeleteTestCase(ctx, req)
	if err == nil {
		cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
		_ = s.RedisCacheClient.Delete(cacheKey)
	}
	return resp, err
}

// getlanguagesupports retrieves supported languages for a problem
func (s *ProblemService) GetLanguageSupports(ctx context.Context, req *pb.GetLanguageSupportsRequest) (*pb.GetLanguageSupportsResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	cacheKey := fmt.Sprintf("language_supports:%s", req.ProblemId)
	cachedLangs, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedLangs != nil {
		var langs pb.GetLanguageSupportsResponse
		cachedStr, ok := cachedLangs.(string)
		if !ok {
			fmt.Println("failed to assert cached languages to string")
			goto fetchFromDB
		}
		if err := json.Unmarshal([]byte(cachedStr), &langs); err == nil {
			return &langs, nil
		}
	}
fetchFromDB:
	resp, err := s.RepoConnInstance.GetLanguageSupports(ctx, req)
	if err != nil {
		return nil, err
	}
	langsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, langsBytes, 30*time.Minute); err != nil {
		fmt.Printf("failed to cache language supports: %v\n", err)
	}
	return resp, nil
}

// fullvalidationbyproblemid validates a problem across all supported languages
func (s *ProblemService) FullValidationByProblemID(ctx context.Context, req *pb.FullValidationByProblemIDRequest) (*pb.FullValidationByProblemIDResponse, error) {
	if req.ProblemId == "" {
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
		s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
		return data, s.createGrpcError(codes.Unimplemented, errMsg, data.ErrorType, err)
	}

	for _, lang := range problem.SupportedLanguages {
		validateCode, ok := problem.ValidateCode[lang]
		if !ok {
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
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("Execution failed for language %s: %v", lang, err),
				ErrorType: "EXECUTION_ERROR",
			}, s.createGrpcError(codes.Internal, "Execution error", "EXECUTION_ERROR", err)
		}

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(res.Message), &result); err != nil {
			return nil, fmt.Errorf("failed to parse execution result: %w", err)
		}

		overallPass, ok := result["overallPass"].(bool)
		if !ok {
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("No output received for language %s", lang),
				ErrorType: "EXECUTION_ERROR",
			}, s.createGrpcError(codes.Internal, "Invalid execution result "+fmt.Sprintf("No output received for language %s", lang), "EXECUTION_ERROR", nil)
		}

		if !overallPass {
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("Validation failed for language %s", lang),
				ErrorType: "VALIDATION_FAILED",
			}, s.createGrpcError(codes.FailedPrecondition, "Validation failed", "VALIDATION_FAILED", nil)
		}
	}

	fmt.Println(req.ProblemId)
	status := s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, true)
	message := "Full Validation Successful"
	if !status {
		s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
		message = "Full Validation completed, but failed to toggle status"
	}
	cacheKey := fmt.Sprintf("problem:%s", req.ProblemId)
	_ = s.RedisCacheClient.Delete(cacheKey)
	return &pb.FullValidationByProblemIDResponse{
		Success:   status,
		Message:   message,
		ErrorType: "",
	}, nil
}

// getsubmissionsbyoptionalproblemid retrieves submissions
func (s *ProblemService) GetSubmissionsByOptionalProblemID(ctx context.Context, req *pb.GetSubmissionsRequest) (*pb.GetSubmissionsResponse, error) {
	if *req.ProblemId == "" && req.UserId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and user ID are required", "VALIDATION_ERROR", nil)
	}
	cacheKey := fmt.Sprintf("submissions:%s:%s", *req.ProblemId, req.UserId)
	cachedSubmissions, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedSubmissions != nil {
		var submissions pb.GetSubmissionsResponse
		cachedStr, ok := cachedSubmissions.(string)
		if !ok {
			fmt.Println("failed to assert cached submissions to string")
			goto fetchFromDB
		}
		if err := json.Unmarshal([]byte(cachedStr), &submissions); err == nil {
			return &submissions, nil
		}
	}
fetchFromDB:
	resp, err := s.RepoConnInstance.GetSubmissionsByOptionalProblemID(ctx, req)
	if err != nil {
		return nil, err
	}
	submissionsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, submissionsBytes, 5*time.Minute); err != nil {
		fmt.Printf("failed to cache submissions: %v\n", err)
	}
	return resp, nil
}

// getproblembyidslug retrieves a problem by id or slug
func (s *ProblemService) GetProblemByIDSlug(ctx context.Context, req *pb.GetProblemByIdSlugRequest) (*pb.GetProblemByIdSlugResponse, error) {
	if req.ProblemId == "" && req.Slug == nil {
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
			fmt.Println("failed to assert cached problem to string")
			goto fetchFromDB
		}
		if err := json.Unmarshal([]byte(cachedStr), &problem); err == nil {
			return &problem, nil
		}
	}
fetchFromDB:
	resp, err := s.RepoConnInstance.GetProblemByIDSlug(ctx, req)
	if err != nil {
		return nil, err
	}
	problemBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, problemBytes, 1*time.Hour); err != nil {
		fmt.Printf("failed to cache problem by id/slug: %v\n", err)
	}
	return resp, nil
}

// getproblembyidlist retrieves problems by id list
func (s *ProblemService) GetProblemMetadataList(ctx context.Context, req *pb.GetProblemMetadataListRequest) (*pb.GetProblemMetadataListResponse, error) {
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
			fmt.Println("failed to assert cached problems to string")
			goto fetchFromDB
		}
		if err := json.Unmarshal([]byte(cachedStr), &problems); err == nil {
			return &problems, nil
		}
	}
fetchFromDB:
	resp, err := s.RepoConnInstance.GetProblemByIDList(ctx, req)
	if err != nil {
		return nil, err
	}
	problemsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, problemsBytes, 1*time.Hour); err != nil {
		fmt.Printf("failed to cache problem id list: %v\n", err)
	}
	return resp, nil
}

// runusercodeproblem executes user code for a problem
func (s *ProblemService) RunUserCodeProblem(ctx context.Context, req *pb.RunProblemRequest) (*pb.RunProblemResponse, error) {
	// Log start of the function call
	log.Printf("Running user code for problem ID: %s, Language: %s", req.ProblemId, req.Language)

	// Retrieve problem data
	problem, err := s.RepoConnInstance.GetProblem(ctx, &pb.GetProblemRequest{ProblemId: req.ProblemId})
	if err != nil {
		log.Printf("Error fetching problem %s: %v", req.ProblemId, err)
		return nil, fmt.Errorf("problem not found: %w", err)
	}

	submitCase := !req.IsRunTestcase

	// Check if the language is supported for validation
	validateCode, ok := problem.ValidateCode[req.Language]
	if !ok {
		log.Printf("Invalid language: %s for problem ID: %s", req.Language, req.ProblemId)
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
	// Log the process of collecting test cases
	log.Printf("Collecting test cases for problem ID: %s", req)
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
		log.Printf("Error marshaling test cases: %v", err)
		return nil, fmt.Errorf("failed to marshal test cases: %w", err)
	}

	tmpl := validateCode.Template
	// Prepare the code for the compiler
	if req.Language == "python" || req.Language == "javascript" || req.Language == "py" || req.Language == "js" {
		escaped := strings.ReplaceAll(string(testCasesJSON), `"`, `\"`)
		tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", escaped, 1)
	} else {
		tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", string(testCasesJSON), 1)
	}
	tmpl = strings.Replace(tmpl, "{FUNCTION_PLACEHOLDER}", req.UserCode, 1)

	// Create the compiler request payload
	compilerRequest := map[string]interface{}{
		"code":     tmpl,
		"language": req.Language,
	}
	compilerRequestBytes, err := json.Marshal(compilerRequest)
	if err != nil {
		log.Printf("Error serializing compiler request: %v", err)
		return nil, fmt.Errorf("failed to serialize compiler request: %w", err)
	}

	// Log request for execution
	log.Printf("Sending request to compiler for problem ID: %s", compilerRequest)
	msg, err := s.NatsClient.Request("problems.execute.request", compilerRequestBytes, 3*time.Second)
	if err != nil {
		log.Printf("Error executing code for problem ID: %s, Error: %v", req.ProblemId, err)
		return &pb.RunProblemResponse{
			Success:       false,
			ErrorType:     "COMPILATION_ERROR",
			Message:       "Failed to execute code",
			ProblemId:     req.ProblemId,
			Language:      req.Language,
			IsRunTestcase: req.IsRunTestcase,
		}, nil
	}

	var result map[string]interface{}
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		log.Printf("Error parsing execution result for problem ID: %s, Error: %v", req.ProblemId, err)
		return nil, fmt.Errorf("failed to parse execution result: %w", err)
	}

	output, ok := result["output"].(string)
	if !ok {
		log.Printf("Invalid execution result format for problem ID: %s", req.ProblemId)
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
		log.Printf("Compilation error in user code for problem ID: %s, Error: %s", req.ProblemId, output)
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

	log.Printf("Execution result for problem ID: %s: %+v", req.ProblemId, executionStatsResult)
	status := "FAILED"
	if executionStatsResult.OverallPass {
		status = "SUCCESS"
	}

	s.processSubmission(ctx, req, status, submitCase, *problem, req.UserCode)
	if submitCase && req.UserId != "" {
		cacheKey := fmt.Sprintf("submissions:%s:%s", req.ProblemId, req.UserId)
		_ = s.RedisCacheClient.Delete(cacheKey)
		cacheKey = fmt.Sprintf("stats:%s", req.UserId)
		_ = s.RedisCacheClient.Delete(cacheKey)
	}

	// Log the successful completion of the process
	log.Printf("Completed user code execution for problem ID: %s, Status: %s", req.ProblemId, status)

	return &pb.RunProblemResponse{
		Success:       true,
		ProblemId:     req.ProblemId,
		Language:      req.Language,
		IsRunTestcase: req.IsRunTestcase,
		Message:       output,
	}, nil
}

// processsubmission handles submission processing
func (s *ProblemService) processSubmission(ctx context.Context, req *pb.RunProblemRequest, status string, submitCasePass bool, problem model.Problem, userCode string) {
	if !submitCasePass || req.UserId == "" {
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

	s.RepoConnInstance.PushSubmissionData(ctx, &submission, status)
	cacheKey := fmt.Sprintf("submissions:%s:%s", req.ProblemId, req.UserId)
	_ = s.RedisCacheClient.Delete(cacheKey)
	
	cacheKey = fmt.Sprintf("heatmap:%s:%d:%d", req.UserId, time.Now().Year(), time.Now().Month())
	_=s.RedisCacheClient.Delete(cacheKey)

	cacheKey = fmt.Sprintf("stats:%s", req.UserId)
	_ = s.RedisCacheClient.Delete(cacheKey)
}

// getproblemsdonestatistics retrieves problem completion stats
func (s *ProblemService) GetProblemsDoneStatistics(ctx context.Context, req *pb.GetProblemsDoneStatisticsRequest) (*pb.GetProblemsDoneStatisticsResponse, error) {
	fmt.Println(s.RepoConnInstance.GetMonthlyContributionHistory(req.UserId, 0, 0))

	cacheKey := fmt.Sprintf("stats:%s", req.UserId)
	cachedStats, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedStats != nil {
		var stats pb.GetProblemsDoneStatisticsResponse
		cachedStr, ok := cachedStats.(string)
		if !ok {
			fmt.Println("failed to assert cached stats to string")
			goto fetchFromDB
		}
		if err := json.Unmarshal([]byte(cachedStr), &stats); err == nil {
			return &stats, nil
		}
	}
fetchFromDB:
	data, err := s.RepoConnInstance.ProblemsDoneStatistics(req.UserId)
	if err != nil {
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

	statsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, statsBytes, 1*time.Hour); err != nil {
		fmt.Printf("failed to cache problem stats: %v\n", err)
	}
	return resp, err
}

func (s *ProblemService) GetMonthlyActivityHeatmap(ctx context.Context, req *pb.GetMonthlyActivityHeatmapRequest) (*pb.GetMonthlyActivityHeatmapResponse, error) {
	cacheKey := fmt.Sprintf("heatmap:%s:%d:%d", req.UserID, req.Year, req.Month)
	cachedData, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedData != nil {
		cachedStr, ok := cachedData.(string)
		if !ok {
			fmt.Println("failed to assert cached data to string")
		} else {
			var heatmap pb.GetMonthlyActivityHeatmapResponse
			if err := json.Unmarshal([]byte(cachedStr), &heatmap); err == nil {
				return &heatmap, nil
			}
		}
	}

	data, err := s.RepoConnInstance.GetMonthlyContributionHistory(req.UserID, int(req.Month), int(req.Year))
	if err != nil {
		return nil, err
	}

	resp := &pb.GetMonthlyActivityHeatmapResponse{
		Data: make([]*pb.ActivityDay, len(data.Data)),
	}
	for i, day := range data.Data {
		resp.Data[i] = &pb.ActivityDay{
			Date:     day.Date, // Fixed to use day.Date
			Count:    int32(day.Count),
			IsActive: day.IsActive,
		}
	}

	now := time.Now()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	ttl := time.Until(nextMidnight)

	heatmapBytes, err := json.Marshal(resp)
	if err != nil {
		fmt.Printf("failed to marshal heatmap response: %v\n", err)
	} else if err := s.RedisCacheClient.Set(cacheKey, heatmapBytes, ttl); err != nil {
		fmt.Printf("failed to cache monthly activity heatmap: %v\n", err)
	}

	return resp, nil
}

// mapproblemstatistics converts model stats to pb stats
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

func (s *ProblemService) GetTopKGlobal(ctx context.Context, req *pb.GetTopKGlobalRequest) (*pb.GetTopKGlobalResponse, error) {
	startRedis := time.Now()
	users, err := s.LB.GetTopKGlobal()
	if err == nil && len(users) > 0 {
		log.Printf("REDIS TIME | GetTopKGlobal: %v", time.Since(startRedis))
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
	log.Printf("REDIS MISS | GetTopKGlobal: %v", time.Since(startRedis))

	startMongo := time.Now()
	k := int(req.K)
	if k == 0 {
		k = 10
	}
	mongoUsers, err := s.RepoConnInstance.GetTopKGlobalMongo(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch top K global from MongoDB: %w", err)
	}
	log.Printf("MONGO TIME | GetTopKGlobal: %v", time.Since(startMongo))

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
	return resp, nil
}

func (s *ProblemService) GetTopKEntity(ctx context.Context, req *pb.GetTopKEntityRequest) (*pb.GetTopKEntityResponse, error) {
	startRedis := time.Now()
	users, err := s.LB.GetTopKEntity(req.Entity)
	if err == nil && len(users) > 0 {
		log.Printf("REDIS TIME | GetTopKEntity: %v", time.Since(startRedis))
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
	log.Printf("REDIS MISS | GetTopKEntity: %v", time.Since(startRedis))

	startMongo := time.Now()
	mongoUsers, err := s.RepoConnInstance.GetTopKEntityMongo(ctx, req.Entity, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch top K entity from MongoDB: %w", err)
	}
	log.Printf("MONGO TIME | GetTopKEntity: %v", time.Since(startMongo))

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
	return resp, nil
}

func (s *ProblemService) GetUserRank(ctx context.Context, req *pb.GetUserRankRequest) (*pb.GetUserRankResponse, error) {
	startRedis := time.Now()
	globalRank, err := s.LB.GetRankGlobal(req.UserId)
	if err == nil {
		entityRank, err := s.LB.GetRankEntity(req.UserId)
		if err == nil {
			log.Printf("REDIS TIME | GetUserRank: %v", time.Since(startRedis))
			return &pb.GetUserRankResponse{
				GlobalRank: int32(globalRank),
				EntityRank: int32(entityRank),
			}, nil
		}
	}
	log.Printf("REDIS MISS | GetUserRank: %v", time.Since(startRedis))

	startMongo := time.Now()
	globalRank, entityRank, err := s.RepoConnInstance.GetUserRankMongo(ctx, req.UserId)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user rank from MongoDB: %w", err)
	}
	log.Printf("MONGO TIME | GetUserRank: %v", time.Since(startMongo))

	return &pb.GetUserRankResponse{
		GlobalRank: int32(globalRank),
		EntityRank: int32(entityRank),
	}, nil
}

func (s *ProblemService) GetLeaderboardData(ctx context.Context, req *pb.GetLeaderboardDataRequest) (*pb.GetLeaderboardDataResponse, error) {
	startRedis := time.Now()
	data, err := s.LB.GetUserLeaderboardData(req.UserId)
	// fmt.Println(data)
	if err == nil {
		log.Printf("REDIS TIME | GetLeaderboardData: %v", time.Since(startRedis))
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
	log.Printf("REDIS MISS | GetLeaderboardData: %v", time.Since(startRedis))

	startMongo := time.Now()
	mongoData, err := s.RepoConnInstance.GetLeaderboardDataMongo(ctx, req.UserId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errors.New("user not found")
		}
		return nil, fmt.Errorf("failed to fetch leaderboard data from MongoDB: %w", err)
	}

	// Fetch additional MongoDB data for ranks and top K
	globalRank, entityRank, err := s.RepoConnInstance.GetUserRankMongo(ctx, req.UserId)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user ranks from MongoDB: %w", err)
	}
	topKGlobal, err := s.RepoConnInstance.GetTopKGlobalMongo(ctx, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch top K global from MongoDB: %w", err)
	}
	var topKEntity []model.UserScore
	if mongoData.Entity != "" {
		topKEntity, err = s.RepoConnInstance.GetTopKEntityMongo(ctx, mongoData.Entity, 10)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch top K entity from MongoDB: %w", err)
		}
	}

	log.Printf("MONGO TIME | GetLeaderboardData: %v", time.Since(startMongo))

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
			// ProblemsDoneCount:user.ProblemsDoneCount,
		}
	}
	for i, user := range topKEntity {
		resp.TopKEntity[i] = &pb.UserScore{
			UserId: user.ID,
			Score:  user.Score,
			Entity: user.Entity,
			// ProblemsDoneCount: user.ProblemsDoneCount,
		}
	}
	return resp, nil
}

func (s *ProblemService) CreateChallenge(ctx context.Context, req *pb.CreateChallengeRequest) (*pb.CreateChallengeResponse, error) {
	if req.Title == "" || req.CreatorId == "" || req.Difficulty == "" || len(req.ProblemIds) == 0 {
		return nil, s.createGrpcError(codes.InvalidArgument, "title, creator_id, difficulty, and at least one problem_id are required", "VALIDATION_ERROR", nil)
	}

	roomCode, err := generateAccessCode(8)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to generate room code", "INTERNAL_ERROR", err)
	}

	password := ""
	if req.IsPrivate {
		pass, err := generateAccessCode(12)
		if err != nil {
			return nil, s.createGrpcError(codes.Internal, "failed to generate password", "INTERNAL_ERROR", err)
		}
		password = pass
	}

	resp, err := s.RepoConnInstance.CreateChallenge(ctx, req, roomCode, password)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to create challenge", "DB_ERROR", err)
	}

	if !req.IsPrivate {
		cacheKey := "challenges:public:*"
		_ = s.RedisCacheClient.Delete(cacheKey)
	}

	return &pb.CreateChallengeResponse{
		Id:       resp.Id,
		Password: password,
		JoinUrl:  fmt.Sprintf("https://xcode.com/challenges/join/%s/%v", resp.Id, resp.Password),
	}, nil
}

func (s *ProblemService) GetChallengeDetails(ctx context.Context, req *pb.GetChallengeDetailsRequest) (*pb.GetChallengeDetailsResponse, error) {
	if req.Id == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id and user_id are required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("challenge_details:%s", req.Id)
	cachedDetails, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedDetails != nil {
		var details pb.GetChallengeDetailsResponse
		if cachedStr, ok := cachedDetails.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &details); err == nil {
				return &details, nil
			}
		}
	}

	challengeResp, err := s.RepoConnInstance.GetChallenge(ctx, &pb.GetChallengeDetailsRequest{Id: req.Id})
	if err != nil {
		return nil, s.createGrpcError(codes.NotFound, "challenge not found", "NOT_FOUND", err)
	}

	leaderboard, err := s.RepoConnInstance.GetChallengeLeaderboard(ctx, req.Id)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to fetch leaderboard", "DB_ERROR", err)
	}

	response := &pb.GetChallengeDetailsResponse{
		Challenge:   challengeResp.Challenge,
		Leaderboard: leaderboard,
	}

	detailsBytes, _ := json.Marshal(response)
	if err := s.RedisCacheClient.Set(cacheKey, detailsBytes, 1*time.Hour); err != nil {
		log.Printf("failed to cache challenge details: %v", err)
	}

	return response, nil
}

func (s *ProblemService) GetPublicChallenges(ctx context.Context, req *pb.GetPublicChallengesRequest) (*pb.GetPublicChallengesResponse, error) {
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
				return &challenges, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetPublicChallenges(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to fetch public challenges", "DB_ERROR", err)
	}

	challengesBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, challengesBytes, 1*time.Hour); err != nil {
		log.Printf("failed to cache public challenges: %v", err)
	}

	return resp, nil
}

func (s *ProblemService) JoinChallenge(ctx context.Context, req *pb.JoinChallengeRequest) (*pb.JoinChallengeResponse, error) {
	if req.ChallengeId == "" || req.UserId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id, user_id are required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.JoinChallenge(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.InvalidArgument, "failed to join challenge", "ACCESS_DENIED", err)
	}

	cacheKey := fmt.Sprintf("challenge_details:%s:*", req.ChallengeId)
	_ = s.RedisCacheClient.Delete(cacheKey)

	return resp, nil
}

func (s *ProblemService) StartChallenge(ctx context.Context, req *pb.StartChallengeRequest) (*pb.StartChallengeResponse, error) {
	if req.ChallengeId == "" || req.UserId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id and user_id are required", "VALIDATION_ERROR", nil)
	}

	resp, err := s.RepoConnInstance.StartChallenge(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.InvalidArgument, "failed to start challenge", "INVALID_STATE", err)
	}

	cacheKey := fmt.Sprintf("challenge_details:%s:*", req.ChallengeId)
	_ = s.RedisCacheClient.Delete(cacheKey)
	cacheKey = "challenges:public:*"
	_ = s.RedisCacheClient.Delete(cacheKey)

	return resp, nil
}

func (s *ProblemService) EndChallenge(ctx context.Context, req *pb.EndChallengeRequest) (*pb.EndChallengeResponse, error) {
	if req.ChallengeId == "" || req.UserId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id and user_id are required", "VALIDATION_ERROR", nil)
	}

	leaderboard, err := s.RepoConnInstance.GetChallengeLeaderboard(ctx, req.ChallengeId)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to fetch leaderboard", "DB_ERROR", err)
	}

	resp, err := s.RepoConnInstance.EndChallenge(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.InvalidArgument, "failed to end challenge", "INVALID_STATE", err)
	}

	cacheKey := fmt.Sprintf("challenge_details:%s:*", req.ChallengeId)
	_ = s.RedisCacheClient.Delete(cacheKey)
	cacheKey = "challenges:public:*"
	_ = s.RedisCacheClient.Delete(cacheKey)

	return &pb.EndChallengeResponse{
		Success:     resp.Success,
		Leaderboard: leaderboard,
	}, nil
}

func (s *ProblemService) GetSubmissionStatus(ctx context.Context, req *pb.GetSubmissionStatusRequest) (*pb.GetSubmissionStatusResponse, error) {
	if req.SubmissionId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "submission_id is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("submission:%s", req.SubmissionId)
	cachedSubmission, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedSubmission != nil {
		var submission pb.GetSubmissionStatusResponse
		if cachedStr, ok := cachedSubmission.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &submission); err == nil {
				return &submission, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetSubmissionStatus(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.NotFound, "submission not found", "NOT_FOUND", err)
	}

	submissionBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, submissionBytes, 30*time.Minute); err != nil {
		log.Printf("failed to cache submission: %v", err)
	}

	return resp, nil
}

func (s *ProblemService) GetChallengeSubmissions(ctx context.Context, req *pb.GetChallengeSubmissionsRequest) (*pb.GetChallengeSubmissionsResponse, error) {
	if req.ChallengeId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("challenge_submissions:%s", req.ChallengeId)
	cachedSubmissions, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedSubmissions != nil {
		var submissions pb.GetChallengeSubmissionsResponse
		if cachedStr, ok := cachedSubmissions.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &submissions); err == nil {
				return &submissions, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetChallengeSubmissions(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to fetch submissions", "DB_ERROR", err)
	}

	submissionsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, submissionsBytes, 30*time.Minute); err != nil {
		log.Printf("failed to cache challenge submissions: %v", err)
	}

	return resp, nil
}

func (s *ProblemService) GetUserStats(ctx context.Context, req *pb.GetUserStatsRequest) (*pb.GetUserStatsResponse, error) {
	if req.UserId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "user_id is required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("user_stats:%s", req.UserId)
	cachedStats, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedStats != nil {
		var stats pb.GetUserStatsResponse
		if cachedStr, ok := cachedStats.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &stats); err == nil {
				return &stats, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetUserStats(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to fetch user stats", "DB_ERROR", err)
	}

	statsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, statsBytes, 1*time.Hour); err != nil {
		log.Printf("failed to cache user stats: %v", err)
	}

	return resp, nil
}

func (s *ProblemService) GetChallengeUserStats(ctx context.Context, req *pb.GetChallengeUserStatsRequest) (*pb.GetChallengeUserStatsResponse, error) {
	if req.ChallengeId == "" || req.UserId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "challenge_id and user_id are required", "VALIDATION_ERROR", nil)
	}

	cacheKey := fmt.Sprintf("challenge_user_stats:%s:%s", req.ChallengeId, req.UserId)
	cachedStats, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedStats != nil {
		var stats pb.GetChallengeUserStatsResponse
		if cachedStr, ok := cachedStats.(string); ok {
			if err := json.Unmarshal([]byte(cachedStr), &stats); err == nil {
				return &stats, nil
			}
		}
	}

	resp, err := s.RepoConnInstance.GetChallengeUserStats(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to fetch challenge user stats", "DB_ERROR", err)
	}

	statsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, statsBytes, 1*time.Hour); err != nil {
		log.Printf("failed to cache challenge user stats: %v", err)
	}

	return resp, nil
}

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
	if req.UserId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "user_id is required", "VALIDATION_ERROR", nil)
	}

	// Set default pagination values
	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = 10
	}

	// Check cache
// 	cacheKey := fmt.Sprintf("user_challenge_history:%s:%d:%d", req.UserId, req.Page, req.PageSize)
// 	cachedChallenges, err := s.RedisCacheClient.Get(cacheKey)
// 	if err == nil && cachedChallenges != nil {
// 		var challenges pb.GetChallengeHistoryResponse
// 		cachedStr, ok := cachedChallenges.(string)
// 		if !ok {
// 			log.Println("failed to assert cached challenge history to string")
// 			goto fetchFromDB
// 		}
// 		if err := json.Unmarshal([]byte(cachedStr), &challenges); err == nil {
// 			return &challenges, nil
// 		}
// 	}

// fetchFromDB:
	// Fetch from repository
	resp, err := s.RepoConnInstance.GetChallengeHistory(ctx, req)
	if err != nil {
		return nil, s.createGrpcError(codes.Internal, "failed to fetch challenge history", "DB_ERROR", err)
	}

	// // Cache the response
	// challengesBytes, err := json.Marshal(resp)
	// if err == nil {
	// 	if err := s.RedisCacheClient.Set(cacheKey, challengesBytes, 1*time.Hour); err != nil {
	// 		log.Printf("failed to cache user challenge history: %v", err)
	// 	}
	// } else {
	// 	log.Printf("failed to marshal challenge history response: %v", err)
	// }

	return resp, nil
}
