package service

import (
	"context"
	"fmt"
	"xcode/repository"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ProblemService struct {
	RepoConnInstance repository.Repository
	pb.UnimplementedProblemsServiceServer
}

func NewService(repo *repository.Repository) *ProblemService {
	return &ProblemService{
		RepoConnInstance: *repo,
	}
}

// createGrpcError constructs a gRPC error with structured message
func (s *ProblemService) createGrpcError(code codes.Code, message string, errorType string, cause error) error {
	var details string
	if cause != nil {
		details = cause.Error()
	} else {
		details = message
	}
	// Format: "ErrorType: <type>, Code: <code>, Details: <details>"
	errorMessage := fmt.Sprintf("ErrorType: %s, Code: %d, Details: %s", errorType, code, details)
	return status.Error(code, errorMessage)
}


func (s *ProblemService) CreateProblem(ctx context.Context, req *pb.CreateProblemRequest) (*pb.CreateProblemResponse, error) {
	if req.Title == "" || req.Description == "" || req.Difficulty == "" {
		return &pb.CreateProblemResponse{Success: false, Message: "Title, description, and difficulty are required"}, nil
	}
	return s.RepoConnInstance.CreateProblem(ctx, req)
}

func (s *ProblemService) UpdateProblem(ctx context.Context, req *pb.UpdateProblemRequest) (*pb.UpdateProblemResponse, error) {
	if req.ProblemId == "" {
		return &pb.UpdateProblemResponse{Success: false, Message: "Problem ID is required"}, nil
	}
	return s.RepoConnInstance.UpdateProblem(ctx, req)
}

func (s *ProblemService) DeleteProblem(ctx context.Context, req *pb.DeleteProblemRequest) (*pb.DeleteProblemResponse, error) {
	if req.ProblemId == "" {
		return &pb.DeleteProblemResponse{Success: false, Message: "Problem ID is required"}, nil
	}
	return s.RepoConnInstance.DeleteProblem(ctx, req)
}

func (s *ProblemService) GetProblem(ctx context.Context, req *pb.GetProblemRequest) (*pb.GetProblemResponse, error) {
	if req.ProblemId == "" {
		return &pb.GetProblemResponse{}, nil
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
		return &pb.AddTestCasesResponse{Success: false, Message: "Problem ID is required"}, nil
	}
	if len(req.Testcases.Run) == 0 && len(req.Testcases.Submit) == 0 {
		return &pb.AddTestCasesResponse{Success: false, Message: "At least one test case is required"}, nil
	}
	return s.RepoConnInstance.AddTestCases(ctx, req)
}

func (s *ProblemService) RemoveTestCases(ctx context.Context, req *pb.RemoveTestCasesRequest) (*pb.RemoveTestCasesResponse, error) {
	if req.ProblemId == "" {
		return &pb.RemoveTestCasesResponse{Success: false, Message: "Problem ID is required"}, nil
	}
	if len(req.RunIndices) == 0 && len(req.SubmitIndices) == 0 {
		return &pb.RemoveTestCasesResponse{Success: false, Message: "At least one index to remove is required"}, nil
	}
	return s.RepoConnInstance.RemoveTestCases(ctx, req)
}

func (s *ProblemService) AddLanguageSupport(ctx context.Context, req *pb.AddLanguageSupportRequest) (*pb.AddLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return &pb.AddLanguageSupportResponse{Success: false, Message: "Problem ID and language are required"}, nil
	}
	if req.ValidationCode == nil || req.ValidationCode.Code == "" {
		return &pb.AddLanguageSupportResponse{Success: false, Message: "Validation code is required"}, nil
	}
	return s.RepoConnInstance.AddLanguageSupport(ctx, req)
}

func (s *ProblemService) UpdateLanguageSupport(ctx context.Context, req *pb.UpdateLanguageSupportRequest) (*pb.UpdateLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return &pb.UpdateLanguageSupportResponse{Success: false, Message: "Problem ID and language are required"}, nil
	}
	if req == nil || req.Validate.Code == "" {
		return &pb.UpdateLanguageSupportResponse{Success: false, Message: "Validation code is required"}, nil
	}
	return s.RepoConnInstance.UpdateLanguageSupport(ctx, req)
}

func (s *ProblemService) RemoveLanguageSupport(ctx context.Context, req *pb.RemoveLanguageSupportRequest) (*pb.RemoveLanguageSupportResponse, error) {
	if req.ProblemId == "" || req.Language == "" {
		return &pb.RemoveLanguageSupportResponse{Success: false, Message: "Problem ID and language are required"}, nil
	}
	return s.RepoConnInstance.RemoveLanguageSupport(ctx, req)
}

func (s *ProblemService) FullValidationByProblemID(ctx context.Context, req *pb.FullValidationByProblemIDRequest) (*pb.FullValidationByProblemIDResponse, error) {
	if req.ProblemId == "" {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Problem ID is required", ErrorType: "INVALID_ID"}, nil
	}
	return s.RepoConnInstance.FullValidationByProblemID(ctx, req)
}