package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"xcode/model"
	"xcode/natsclient"
	"xcode/repository"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
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
	return s.RepoConnInstance.GetProblem(ctx, req)
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

	if req.ProblemId == "" {
		return &pb.FullValidationByProblemIDResponse{
			Success:   false,
			Message:   "Problem ID is required",
			ErrorType: "VALIDATION_ERROR",
		}, s.createGrpcError(codes.InvalidArgument, "Problem ID is required", "VALIDATION_ERROR", nil)

	}

	data, problem, err := s.RepoConnInstance.BasicValidationByProblemID(ctx, req)
	// fmt.Println(data, err)
	if !data.Success {
		return data, s.createGrpcError(codes.Unimplemented, data.Message, data.ErrorType, err)
	}

	for _, v := range problem.SupportedLanguages {
		res, _ := s.RunUserCodeProblem(ctx, &pb.RunProblemRequest{
			ProblemId:     req.ProblemId,
			UserCode:      problem.ValidateCode[v].Code,
			Language:      v,
			IsRunTestcase: true,
		})
		if !res.Success {
			return data, s.createGrpcError(codes.Unimplemented, res.Message, res.ErrorType, errors.New(res.Message))
		}
	}

	fmt.Println(req.ProblemId)
	status:= s.RepoConnInstance.ToggleProblemValidaition(ctx, req.ProblemId, true)
	message := "Full Valdation Successfull"
	if !status{
		message = "Full Validaition is Done, but failed to toggle status"
	}
	return &pb.FullValidationByProblemIDResponse{Success: status, Message: message, ErrorType: ""}, nil
}

func (s *ProblemService) GetSubmissions(ctx context.Context, req *pb.GetSubmissionsRequest) (*pb.GetSubmissionsResponse, error) {
	if *req.ProblemId == "" || req.UserId == "" {
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
	// Fetch the problem details
	problem, err := s.RepoConnInstance.GetProblem(ctx, &pb.GetProblemRequest{ProblemId: req.ProblemId})
	if err != nil {
		return nil, fmt.Errorf("problem not found: %w", err)
	}

	// fmt.Println(problem)

	// Validate the requested language
	validateCode, ok := problem.Problem.ValidateCode[req.Language]
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
		for _, tc := range problem.Problem.Testcases.Run {
			if tc.Id != "" {
				testCases = append(testCases, model.TestCase{
					ID:       tc.Id,
					Input:    tc.Input,
					Expected: tc.Expected,
				})
			}
		}
	} else {
		for _, tc := range append(problem.Problem.Testcases.Run, problem.Problem.Testcases.Submit...) {
			if tc.Id != "" {
				testCases = append(testCases, model.TestCase{
					ID:       tc.Id,
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

	// fmt.Println(string(testCasesJSON))
	// testCasesJSON = []byte(strings.Replace(string(testCasesJSON), `/"`, `//"`, -1))
	// fmt.Println(string(testCasesJSON))

	// Replace placeholders in the template
	tmpl := validateCode.Template
	if req.Language == "python" || req.Language == "javascript" ||req.Language =="py" || req.Language == "js"{
		escaped := strings.ReplaceAll(string(testCasesJSON), `"`, `\"`)
		tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", escaped, 1)
	} else {
		// escaped := strings.ReplaceAll(string(testCasesJSON), `"`, `\"`)
		tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", string(testCasesJSON), 1)
	}

	tmpl = strings.Replace(tmpl, "{FUNCTION_PLACEHOLDER}", req.UserCode, 1)
	// tmpl = strings.Replace(tmpl, "{TESTCASE_PLACEHOLDER}", string(testCasesJSON), 1)

	// fmt.Println(tmpl)

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
	msg, err := s.NatsClient.Request("problems.execute.request", compilerRequestBytes, 15*time.Second)
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

	// Parse the response
	var result map[string]interface{}
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse execution result: %w", err)
	}

	// Extract the output
	output, ok := result["output"].(string)
	if !ok {
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
		return &pb.RunProblemResponse{
			Success:       false,
			ErrorType:     "COMPILATION_ERROR",
			Message:       output,
			ProblemId:     req.ProblemId,
			Language:      req.Language,
			IsRunTestcase: req.IsRunTestcase,
		}, nil
	}

	return &pb.RunProblemResponse{
		Success:       true,
		ProblemId:     req.ProblemId,
		Language:      req.Language,
		IsRunTestcase: req.IsRunTestcase,
		Message:       output,
	}, nil
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
