package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"xcode/model"
	"xcode/natsclient"
	"xcode/repository"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ProblemService struct {
	RepoConnInstance repository.Repository
	NatsClient       *natsclient.NatsClient
	pb.UnimplementedProblemsServiceServer
}

func NewService(repo *repository.Repository, natsClient *natsclient.NatsClient) *ProblemService {
	return &ProblemService{RepoConnInstance: *repo, NatsClient: natsClient}
}

func (s *ProblemService) GetService() *ProblemService {
	return s
}

func (s *ProblemService) createGrpcError(code codes.Code, message string, errorType string, cause error) error {
	details := message
	if cause != nil {
		details = cause.Error()
	}
	return status.Error(code, fmt.Sprintf("ErrorType: %s, Code: %d, Details: %s", errorType, code, details))
}

func (s *ProblemService) CreateProblem(ctx context.Context, req *pb.CreateProblemRequest) (*pb.CreateProblemResponse, error) {
	if req.Title == "" || req.Description == "" || req.Difficulty == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Title, description, and difficulty are required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.CreateProblem(ctx, req)
}

func (s *ProblemService) UpdateProblem(ctx context.Context, req *pb.UpdateProblemRequest) (*pb.UpdateProblemResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.UpdateProblem(ctx, req)
}

func (s *ProblemService) DeleteProblem(ctx context.Context, req *pb.DeleteProblemRequest) (*pb.DeleteProblemResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.DeleteProblem(ctx, req)
}

func (s *ProblemService) GetProblem(ctx context.Context, req *pb.GetProblemRequest) (*pb.GetProblemResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	problemRepoModel, err := s.RepoConnInstance.GetProblem(ctx, req)
	problemPB := repository.ToProblemResponse(*problemRepoModel)
	return problemPB, err
}

func (s *ProblemService) ListProblems(ctx context.Context, req *pb.ListProblemsRequest) (*pb.ListProblemsResponse, error) {
	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = 10
	}
	return s.RepoConnInstance.ListProblems(ctx, req)
}

func (s *ProblemService) AddTestCases(ctx context.Context, req *pb.AddTestCasesRequest) (*pb.AddTestCasesResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	if len(req.Testcases.Run) == 0 && len(req.Testcases.Submit) == 0 {
		return nil, s.createGrpcError(codes.InvalidArgument, "At least one test case is required", "VALIDATION_ERROR", nil)
	}
	fmt.Println(req.Testcases)
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
	return s.RepoConnInstance.AddTestCases(ctx, req)
}

func (s *ProblemService) AddLanguageSupport(ctx context.Context, req *pb.AddLanguageSupportRequest) (*pb.AddLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}
	if req.ValidationCode == nil || req.ValidationCode.Code == "" || req.ValidationCode.Template == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Validation code (code and template) is required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.AddLanguageSupport(ctx, req)
}

func (s *ProblemService) UpdateLanguageSupport(ctx context.Context, req *pb.UpdateLanguageSupportRequest) (*pb.UpdateLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}
	if req.ValidationCode == nil || req.ValidationCode.Code == "" || req.ValidationCode.Template == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Validation code (code and template) is required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.UpdateLanguageSupport(ctx, req)
}

func (s *ProblemService) RemoveLanguageSupport(ctx context.Context, req *pb.RemoveLanguageSupportRequest) (*pb.RemoveLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and language are required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.RemoveLanguageSupport(ctx, req)
}

func (s *ProblemService) DeleteTestCase(ctx context.Context, req *pb.DeleteTestCaseRequest) (*pb.DeleteTestCaseResponse, error) {
	if req.ProblemId == "" || req.TestcaseId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and testcase ID are required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.DeleteTestCase(ctx, req)
}

func (s *ProblemService) GetLanguageSupports(ctx context.Context, req *pb.GetLanguageSupportsRequest) (*pb.GetLanguageSupportsResponse, error) {
	if req.ProblemId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.GetLanguageSupports(ctx, req)
}

//Error:   &model.ErrorInfo{ErrorType: resp.ErrorType, Code: http.StatusBadRequest, Message: resp.Message, Details: grpcStatus.Message()},

func (s *ProblemService) FullValidationByProblemID(ctx context.Context, req *pb.FullValidationByProblemIDRequest) (*pb.FullValidationByProblemIDResponse, error) {
	// Validate required field
	if req.ProblemId == "" {
		return &pb.FullValidationByProblemIDResponse{
			Success:   false,
			Message:   "Problem ID is required",
			ErrorType: "VALIDATION_ERROR",
		}, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)
	}

	// Perform initial validation
	data, problem, err := s.RepoConnInstance.BasicValidationByProblemID(ctx, req)
	if err != nil || !data.Success {
		errMsg := data.Message
		if errMsg == "" {
			errMsg = "Basic validation failed"
		}
		s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
		return data, s.createGrpcError(codes.Unimplemented, errMsg, data.ErrorType, err)
	}

	// Validate each supported language
	for _, lang := range problem.SupportedLanguages {
		// Ensure validate code exists for the language
		validateCode, ok := problem.ValidateCode[lang]
		if !ok {
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("No validation code found for language: %s", lang),
				ErrorType: "CONFIGURATION_ERROR",
			}, s.createGrpcError(codes.InvalidArgument, "Missing validation code", "CONFIGURATION_ERROR", nil)
		}

		// Run validation for the language
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

		fmt.Println("result ", result)

		// Safely handle the result output
		overallPass, ok := result["overallPass"].(bool)
		fmt.Println("overallPass ", overallPass)
		if !ok {
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("No output received for language %s", lang),
				ErrorType: "EXECUTION_ERROR",
			}, s.createGrpcError(codes.Internal, "Invalid execution result "+fmt.Sprintf("No output received for language %s", lang), "EXECUTION_ERROR", nil)
		}

		// Check validation result based on overallPass
		if !overallPass {
			s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
			return &pb.FullValidationByProblemIDResponse{
				Success:   false,
				Message:   fmt.Sprintf("Validation failed for language %s", lang),
				ErrorType: "VALIDATION_FAILED",
			}, s.createGrpcError(codes.FailedPrecondition, "Validation failed", "VALIDATION_FAILED", nil)
		}
	}

	// Toggle validation status
	fmt.Println(req.ProblemId) // Consider replacing with structured logging
	status := s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, true)
	message := "Full Validation Successful"
	if !status {
		s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, false)
		message = "Full Validation completed, but failed to toggle status"
	}
	return &pb.FullValidationByProblemIDResponse{
		Success:   status,
		Message:   message,
		ErrorType: "",
	}, nil
}

func (s *ProblemService) GetSubmissionsByOptionalProblemID(ctx context.Context, req *pb.GetSubmissionsRequest) (*pb.GetSubmissionsResponse, error) {
	if *req.ProblemId == "" && req.UserId == "" {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID and user ID are required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.GetSubmissionsByOptionalProblemID(ctx, req)
}

func (s *ProblemService) GetProblemByIDSlug(ctx context.Context, req *pb.GetProblemByIdSlugRequest) (*pb.GetProblemByIdSlugResponse, error) {
	if req.ProblemId == "" && req.Slug == nil {
		return nil, s.createGrpcError(codes.InvalidArgument, "Problem ID or slug is required", "VALIDATION_ERROR", nil)
	}
	return s.RepoConnInstance.GetProblemByIDSlug(ctx, req)
}

func (s *ProblemService) GetProblemByIDList(ctx context.Context, req *pb.GetProblemByIdListRequest) (*pb.GetProblemByIdListResponse, error) {
	if req.Page < 1 {
		req.Page = 1
	}
	if req.PageSize < 1 {
		req.PageSize = 10
	}
	return s.RepoConnInstance.GetProblemByIDList(ctx, req)
}

func (s *ProblemService) RunUserCodeProblem(ctx context.Context, req *pb.RunProblemRequest) (*pb.RunProblemResponse, error) {
	//fetch the problem details
	problem, err := s.RepoConnInstance.GetProblem(ctx, &pb.GetProblemRequest{ProblemId: req.ProblemId})
	if err != nil {
		return nil, fmt.Errorf("problem not found: %w", err)
	}

	submitCase := !req.IsRunTestcase

	// Validate the requested language
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

	// Prepare test cases
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

	// Marshal test cases to JSON
	testCasesJSON, err := json.Marshal(testCases)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal test cases: %w", err)
	}

	// Replace placeholders in the template
	tmpl := validateCode.Template
	if req.Language == "python" || req.Language == "javascript" || req.Language == "py" || req.Language == "js" {
		escaped := strings.ReplaceAll(string(testCasesJSON), `"`, `\"`)
		tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", escaped, 1)
	} else {
		tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", string(testCasesJSON), 1)
	}

	tmpl = strings.Replace(tmpl, "{FUNCTION_PLACEHOLDER}", req.UserCode, 1)

	// Prepare the compiler request
	compilerRequest := map[string]interface{}{
		"code":     tmpl,
		"language": req.Language,
	}
	compilerRequestBytes, err := json.Marshal(compilerRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize compiler request: %w", err)
	}

	// Send the request to the NATS client
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

	// map[execution_time:1.230549718s output:{
	// 	"totalTestCases": 32,
	// 	"passedTestCases": 0,
	// 	"failedTestCases": 32,
	// 	"failedTestCase": {
	// 		"testCaseIndex": 0,
	// 		"input": {
	// 			"nums": [
	// 				1,
	// 				2,
	// 				3,
	// 				1
	// 			]
	// 		},
	// 		"expected": true,
	// 		"received": false,
	// 		"passed": false
	// 	},
	// 	"overallPass": false
	// }

	// Parse the response
	// fmt.Println("msg data ",msg.Data)

	var result map[string]interface{}
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse execution result: %w", err)
	}

	// Extract the output
	output, ok1 := result["output"].(string)
	// executionTime,ok2 := result["execution_time"].(string)
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

	// Check for compilation errors
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

	// Final step: Unmarshal output to ExecutionResult
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

	return &pb.RunProblemResponse{
		Success:       true,
		ProblemId:     req.ProblemId,
		Language:      req.Language,
		IsRunTestcase: req.IsRunTestcase,
		Message:       output,
	}, nil
}

//USERID,score,output, execution_time,difficulty, isFirst

func (s *ProblemService) processSubmission(ctx context.Context, req *pb.RunProblemRequest, status string, submitCasePass bool, problem model.Problem, userCode string) {
	if !submitCasePass || req.UserId == "" {
		return
	}

	// type Submission struct {
	// 	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	// 	UserID        string             `bson:"userId" json:"userId"`
	// 	ProblemID     string             `bson:"problemId" json:"problemId"`
	// 	ChallengeID   *string            `bson:"challengeid,omitempty" json:"challengeId,omitempty"`
	// 	Title         string             `bson:"title"  json:"title"`
	// 	SubmittedAt   time.Time          `bson:"submittedAt" json:"submittedAt"`
	// 	Status        string             `bson:"status" json:"status"`
	// 	Score         int                `bson:"score" json:"score"`
	// 	Language      string             `json:"language" bson:"language"`
	// 	Output        string             `json:"output,omitempty" bson:"output"`
	// 	ExecutionTime float64            `json:"executionTime,omitempty" bson:"execution_time"`
	// 	Difficulty    string             `json:"difficulty" bson:"difficulty"`
	// 	IsFirst       bool               `bson:"isFirst" json:"isFirst"`
	// }

	var submission model.Submission
	if req != nil {
		submission = model.Submission{
			ID:            primitive.NewObjectID(),
			UserID:        req.UserId,
			ProblemID:     req.ProblemId,
			ChallengeID:   nil, // for now.
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
}

// [
// 	{
// 		input: {
// 			nums:[1,2],
// 			target:9
// 		},
// 		expected:[0,1]
// 	},
// 	{
// 		input: {
// 			nums:[1,2],
// 			target:9
// 		},
// 		expected:[0,1]
// 	}
// ]

// func (s *ProblemService) GetSubmissionsByOptionalProblemID(ctx context.Context, req *pb.GetSubmissionsRequest) (*pb.GetSubmissionsResponse, error) {


func (s *ProblemService) GetProblemsDoneStatistics(ctx context.Context, req *pb.GetProblemsDoneStatisticsRequest) (*pb.GetProblemsDoneStatisticsResponse,error) {
	data, err := s.RepoConnInstance.ProblemsDoneStatistics(req.UserId)
	return &pb.GetProblemsDoneStatisticsResponse{
			Data: &pb.ProblemsDoneStatistics{
				MaxEasyCount:    data.MaxEasyCount,
				DoneEasyCount:   data.DoneEasyCount,
				MaxMediumCount:  data.MaxMediumCount,
				DoneMediumCount: data.DoneMediumCount,
				MaxHardCount:    data.MaxHardCount,
				DoneHardCount:   data.DoneHardCount,
			},
	},err
}

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