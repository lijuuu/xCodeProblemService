package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"xcode/cache"
	"xcode/model"
	"xcode/natsclient"
	"xcode/repository"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// problemservice handles problem-related operations
type ProblemService struct {
	RepoConnInstance repository.Repository
	NatsClient       *natsclient.NatsClient
	RedisCacheClient cache.RedisCache
	pb.UnimplementedProblemsServiceServer
}

// newservice initializes a new problemservice
func NewService(repo *repository.Repository, natsClient *natsclient.NatsClient, RedisCacheClient cache.RedisCache) *ProblemService {
	return &ProblemService{RepoConnInstance: *repo, NatsClient: natsClient, RedisCacheClient: RedisCacheClient}
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
		if err := json.Unmarshal(cachedProblem.([]byte), &problem); err == nil {
			return &problem, nil
		}
	}
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
		if err := json.Unmarshal(cachedProblems.([]byte), &problems); err == nil {
			return &problems, nil
		}
	}
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
		if err := json.Unmarshal(cachedLangs.([]byte), &langs); err == nil {
			return &langs, nil
		}
	}
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
		if err := json.Unmarshal(cachedSubmissions.([]byte), &submissions); err == nil {
			return &submissions, nil
		}
	}
	resp, err := s.RepoConnInstance.GetSubmissionsByOptionalProblemID(ctx, req)
	if err != nil {
		return nil, err
	}
	submissionsBytes, _ := json.Marshal(resp)
	if err := s.RedisCacheClient.Set(cacheKey, submissionsBytes, 30*time.Minute); err != nil {
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
		if err := json.Unmarshal(cachedProblem.([]byte), &problem); err == nil {
			return &problem, nil
		}
	}
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
func (s *ProblemService) GetProblemByIDList(ctx context.Context, req *pb.GetProblemByIdListRequest) (*pb.GetProblemByIdListResponse, error) {
	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = 10
	}
	cacheKey := fmt.Sprintf("problem_id_list:%d:%d", req.Page, req.PageSize)
	cachedProblems, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedProblems != nil {
		var problems pb.GetProblemByIdListResponse
		if err := json.Unmarshal(cachedProblems.([]byte), &problems); err == nil {
			return &problems, nil
		}
	}
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
	problem, err := s.RepoConnInstance.GetProblem(ctx, &pb.GetProblemRequest{ProblemId: req.ProblemId})
	if err != nil {
		return nil, fmt.Errorf("problem not found: %w", err)
	}

	submitCase := !req.IsRunTestcase

	validateCode, ok := problem.ValidateCode[req.Language]
	if !ok {
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

	compilerRequest := map[string]interface{}{
		"code":     tmpl,
		"language": req.Language,
	}
	compilerRequestBytes, err := json.Marshal(compilerRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize compiler request: %w", err)
	}

	msg, err := s.NatsClient.Request("problems.execute.request", compilerRequestBytes, 3*time.Second)
	if err != nil {
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
		return nil, fmt.Errorf("failed to parse execution result: %w", err)
	}

	output, ok1 := result["output"].(string)
	if !ok1 {
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

	fmt.Println(executionStatsResult)
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
	cacheKey = fmt.Sprintf("stats:%s", req.UserId)
	_ = s.RedisCacheClient.Delete(cacheKey)
}

// getproblemsdonestatistics retrieves problem completion stats
func (s *ProblemService) GetProblemsDoneStatistics(ctx context.Context, req *pb.GetProblemsDoneStatisticsRequest) (*pb.GetProblemsDoneStatisticsResponse, error) {
	cacheKey := fmt.Sprintf("stats:%s", req.UserId)
	cachedStats, err := s.RedisCacheClient.Get(cacheKey)
	if err == nil && cachedStats != nil {
		var stats pb.GetProblemsDoneStatisticsResponse
		if err := json.Unmarshal(cachedStats.([]byte), &stats); err == nil {
			return &stats, nil
		}
	}
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