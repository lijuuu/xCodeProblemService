package repository

import (
	"context"
	"fmt"
	"time"
	"xcode/model"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Repository struct {
	mongoclientInstance              *mongo.Client
	problemsCollection               *mongo.Collection
	submissionsCollection            *mongo.Collection
	submissionFirstSuccessCollection *mongo.Collection
}

func NewRepository(client *mongo.Client) *Repository {
	return &Repository{
		mongoclientInstance:              client,
		problemsCollection:               client.Database("problems_db").Collection("problems"),
		submissionsCollection:            client.Database("submissions_db").Collection("submissions"),
		submissionFirstSuccessCollection: client.Database("submissions_db").Collection("submissionsfirstsuccess"),
	}
}

func (r *Repository) CreateProblem(ctx context.Context, req *pb.CreateProblemRequest) (*pb.CreateProblemResponse, error) {
	count, err := r.problemsCollection.CountDocuments(ctx, bson.M{"title": req.Title, "deleted_at": nil})
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return &pb.CreateProblemResponse{Success: false, Message: "Problem with this title already exists"}, nil
	}
	now := time.Now()
	problem := model.Problem{
		Title:              req.Title,
		Description:        req.Description,
		Tags:               req.Tags,
		Difficulty:         req.Difficulty,
		CreatedAt:          now,
		UpdatedAt:          now,
		DeletedAt:          nil,
		TestCases:          model.TestCaseCollection{Run: []model.TestCase{}, Submit: []model.TestCase{}},
		SupportedLanguages: []string{},
		ValidateCode:       make(map[string]model.CodeData),
		Validated:          false,
	}
	res, err := r.problemsCollection.InsertOne(ctx, problem)
	if err != nil {
		return nil, err
	}
	return &pb.CreateProblemResponse{ProblemId: res.InsertedID.(primitive.ObjectID).Hex(), Success: true, Message: "Problem created successfully"}, nil
}

func (r *Repository) UpdateProblem(ctx context.Context, req *pb.UpdateProblemRequest) (*pb.UpdateProblemResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return nil, err
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.UpdateProblemResponse{Success: false, Message: "Problem not found or deleted"}, nil
	}
	if err != nil {
		return nil, err
	}
	update := bson.M{"$set": bson.M{"updated_at": time.Now()}}
	resetValidation := false
	if req.Title != nil {
		if *req.Title == "" {
			return &pb.UpdateProblemResponse{Success: false, Message: "Title cannot be empty"}, nil
		}
		count, err := r.problemsCollection.CountDocuments(ctx, bson.M{"title": *req.Title, "_id": bson.M{"$ne": id}, "deleted_at": nil})
		if err != nil {
			return nil, err
		}
		if count > 0 {
			return &pb.UpdateProblemResponse{Success: false, Message: "Another problem with this title already exists"}, nil
		}
		update["$set"].(bson.M)["title"] = *req.Title
		resetValidation = true
	}
	if req.Description != nil {
		if *req.Description == "" {
			return &pb.UpdateProblemResponse{Success: false, Message: "Description cannot be empty"}, nil
		}
		update["$set"].(bson.M)["description"] = *req.Description
		resetValidation = true
	}
	if len(req.Tags) > 0 {
		update["$set"].(bson.M)["tags"] = req.Tags
		resetValidation = true
	}
	if req.Difficulty != nil {
		if *req.Difficulty == "" {
			return &pb.UpdateProblemResponse{Success: false, Message: "Difficulty cannot be empty"}, nil
		}
		update["$set"].(bson.M)["difficulty"] = *req.Difficulty
		resetValidation = true
	}
	if resetValidation {
		update["$set"].(bson.M)["validated"] = false
	}
	result, err := r.problemsCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return &pb.UpdateProblemResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.UpdateProblemResponse{Success: true, Message: "Problem updated successfully"}, nil
}

func (r *Repository) DeleteProblem(ctx context.Context, req *pb.DeleteProblemRequest) (*pb.DeleteProblemResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	update := bson.M{"$set": bson.M{"deleted_at": now, "updated_at": now}}
	result, err := r.problemsCollection.UpdateOne(ctx, bson.M{"_id": id, "deleted_at": nil}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return &pb.DeleteProblemResponse{Success: false, Message: "Problem not found or already deleted"}, nil
	}
	return &pb.DeleteProblemResponse{Success: true, Message: "Problem marked as deleted"}, nil
}

func (r *Repository) GetProblem(ctx context.Context, req *pb.GetProblemRequest) (*model.Problem, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &model.Problem{}, err
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &model.Problem{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &problem, nil
}

func (r *Repository) ListProblems(ctx context.Context, req *pb.ListProblemsRequest) (*pb.ListProblemsResponse, error) {
	filter := bson.M{"deleted_at": nil}
	if len(req.Tags) > 0 {
		filter["tags"] = bson.M{"$all": req.Tags}
	}
	if req.Difficulty != "" {
		filter["difficulty"] = req.Difficulty
	}
	if req.SearchQuery != "" {
		filter["$or"] = []bson.M{
			{"title": bson.M{"$regex": req.SearchQuery, "$options": "i"}},
			{"description": bson.M{"$regex": req.SearchQuery, "$options": "i"}},
		}
	}
	opts := options.Find().SetSkip(int64(req.Page-1) * int64(req.PageSize)).SetLimit(int64(req.PageSize))
	cursor, err := r.problemsCollection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var problems []model.Problem
	if err = cursor.All(ctx, &problems); err != nil {
		return nil, err
	}
	total, err := r.problemsCollection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}
	resp := &pb.ListProblemsResponse{
		Problems:   make([]*pb.Problem, len(problems)),
		TotalCount: int32(total),
		Page:       req.Page,
		PageSize:   req.PageSize,
	}
	for i, p := range problems {
		resp.Problems[i] = ToProblem(p)
	}
	return resp, nil
}

func (r *Repository) AddTestCases(ctx context.Context, req *pb.AddTestCasesRequest) (*pb.AddTestCasesResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return nil, err
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.AddTestCasesResponse{Success: false, Message: "Problem not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(problem.TestCases.Run)+len(req.Testcases.Run) > 3 {
		return &pb.AddTestCasesResponse{Success: false, Message: "Run test case limit (3) exceeded"}, nil
	}
	if len(problem.TestCases.Submit)+len(req.Testcases.Submit) > 100 {
		return &pb.AddTestCasesResponse{Success: false, Message: "Submit test case limit (100) exceeded"}, nil
	}
	existingRunIDs := make(map[string]bool)
	existingSubmitIDs := make(map[string]bool)
	for _, tc := range problem.TestCases.Run {
		existingRunIDs[tc.ID] = true
	}
	for _, tc := range problem.TestCases.Submit {
		existingSubmitIDs[tc.ID] = true
	}
	newRun := toTestCases(req.Testcases.Run, existingRunIDs, true)
	newSubmit := toTestCases(req.Testcases.Submit, existingSubmitIDs, false)
	update := bson.M{
		"$push": bson.M{
			"testcases.run":    bson.M{"$each": newRun},
			"testcases.submit": bson.M{"$each": newSubmit},
		},
		"$set": bson.M{
			"updated_at": time.Now(),
			"validated":  false,
		},
	}
	result, err := r.problemsCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return &pb.AddTestCasesResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.AddTestCasesResponse{
		Success:    true,
		Message:    "Test cases added successfully",
		AddedCount: int32(len(newRun) + len(newSubmit)),
	}, nil
}

func (r *Repository) DeleteTestCase(ctx context.Context, req *pb.DeleteTestCaseRequest) (*pb.DeleteTestCaseResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return nil, err
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.DeleteTestCaseResponse{Success: false, Message: "Problem not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	field := "testcases.submit"
	testcases := problem.TestCases.Submit
	if req.IsRunTestcase {
		field = "testcases.run"
		testcases = problem.TestCases.Run
	}
	found := false
	for _, tc := range testcases {
		if tc.ID == req.TestcaseId {
			found = true
			break
		}
	}
	if !found {
		return &pb.DeleteTestCaseResponse{Success: false, Message: "Testcase not found"}, nil
	}
	update := bson.M{
		"$pull": bson.M{field: bson.M{"id": req.TestcaseId}},
		"$set":  bson.M{"updated_at": time.Now(), "validated": false},
	}
	result, err := r.problemsCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return &pb.DeleteTestCaseResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.DeleteTestCaseResponse{Success: true, Message: "Testcase deleted successfully"}, nil
}

func (r *Repository) AddLanguageSupport(ctx context.Context, req *pb.AddLanguageSupportRequest) (*pb.AddLanguageSupportResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return nil, err
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.AddLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	for _, lang := range problem.SupportedLanguages {
		if lang == req.Language {
			return &pb.AddLanguageSupportResponse{Success: false, Message: "Language already supported"}, nil
		}
	}
	update := bson.M{
		"$push": bson.M{"supported_languages": req.Language},
		"$set": bson.M{
			"validate_code." + req.Language: model.CodeData{
				Placeholder: req.ValidationCode.Placeholder,
				Code:        req.ValidationCode.Code,
				Template:    req.ValidationCode.Template,
			},
			"updated_at": time.Now(),
			"validated":  false,
		},
	}
	result, err := r.problemsCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return &pb.AddLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.AddLanguageSupportResponse{Success: true, Message: "Language support added successfully"}, nil
}

func (r *Repository) UpdateLanguageSupport(ctx context.Context, req *pb.UpdateLanguageSupportRequest) (*pb.UpdateLanguageSupportResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return nil, err
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.UpdateLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	langExists := false
	for _, lang := range problem.SupportedLanguages {
		if lang == req.Language {
			langExists = true
			break
		}
	}
	if !langExists {
		return &pb.UpdateLanguageSupportResponse{Success: false, Message: "Language not supported"}, nil
	}
	update := bson.M{
		"$set": bson.M{
			"validate_code." + req.Language: model.CodeData{
				Placeholder: req.ValidationCode.Placeholder,
				Code:        req.ValidationCode.Code,
				Template:    req.ValidationCode.Template,
			},
			"updated_at": time.Now(),
			"validated":  false,
		},
	}
	result, err := r.problemsCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return &pb.UpdateLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.UpdateLanguageSupportResponse{Success: true, Message: "Language support updated successfully"}, nil
}

func (r *Repository) RemoveLanguageSupport(ctx context.Context, req *pb.RemoveLanguageSupportRequest) (*pb.RemoveLanguageSupportResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return nil, err
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.RemoveLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	langExists := false
	for _, lang := range problem.SupportedLanguages {
		if lang == req.Language {
			langExists = true
			break
		}
	}
	if !langExists {
		return &pb.RemoveLanguageSupportResponse{Success: false, Message: "Language not supported"}, nil
	}
	update := bson.M{
		"$pull":  bson.M{"supported_languages": req.Language},
		"$unset": bson.M{"validate_code." + req.Language: ""},
		"$set":   bson.M{"updated_at": time.Now(), "validated": false},
	}
	result, err := r.problemsCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return &pb.RemoveLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.RemoveLanguageSupportResponse{Success: true, Message: "Language support removed successfully"}, nil
}

func (r *Repository) GetLanguageSupports(ctx context.Context, req *pb.GetLanguageSupportsRequest) (*pb.GetLanguageSupportsResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return nil, err
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.GetLanguageSupportsResponse{Success: false, Message: "Problem not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	validateCode := make(map[string]*pb.ValidationCode)
	for lang, vc := range problem.ValidateCode {
		validateCode[lang] = &pb.ValidationCode{
			Placeholder: vc.Placeholder,
			Code:        vc.Code,
			Template:    vc.Template,
		}
	}
	return &pb.GetLanguageSupportsResponse{
		Success:            true,
		Message:            "Language supports retrieved successfully",
		SupportedLanguages: problem.SupportedLanguages,
		ValidateCode:       validateCode,
	}, nil
}

func (r *Repository) BasicValidationByProblemID(ctx context.Context, req *pb.FullValidationByProblemIDRequest) (*pb.FullValidationByProblemIDResponse, model.Problem, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Invalid problem ID", ErrorType: "INVALID_ID"}, model.Problem{}, nil
	}
	var problem model.Problem
	err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err != nil {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Problem not found", ErrorType: "NOT_FOUND"}, model.Problem{}, nil
	}
	if len(problem.TestCases.Run) < 3 && len(problem.TestCases.Submit) < 5 {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: "requirements not satisifed for len(testcase) >= 3 and len(submitcase) >= 5", ErrorType: "INSUFFICIENT_TESTCASES"}, model.Problem{}, nil
	}
	if len(problem.SupportedLanguages) == 0 {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: "No supported languages", ErrorType: "NO_LANGUAGES"}, model.Problem{}, nil
	}
	for _, lang := range problem.SupportedLanguages {
		if _, ok := problem.ValidateCode[lang]; !ok {
			return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Missing validation code for " + lang, ErrorType: "MISSING_VALIDATION_CODES"}, model.Problem{}, nil
		}
		if problem.ValidateCode[lang].Placeholder == "" {
			return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Missing placeholder for language " + lang, ErrorType: "MISSING_PLACEHOLDER"}, model.Problem{}, nil
		}
		if problem.ValidateCode[lang].Template == "" {
			return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Missing template for language " + lang, ErrorType: "MISSING_TEMPLATE"}, model.Problem{}, nil
		}

		if problem.ValidateCode[lang].Code == "" {
			return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Missing code for language " + lang, ErrorType: "MISSING_CODE"}, model.Problem{}, nil
		}
	}

	return &pb.FullValidationByProblemIDResponse{Success: true, Message: "Basic Validation completed successfully", ErrorType: ""}, problem, nil
}

func (r *Repository) ToggleProblemValidaition(ctx context.Context, problemID string, status bool) bool {
	now := time.Now()
	problemUUID, _ := primitive.ObjectIDFromHex(problemID)
	update := bson.M{"$set": bson.M{"validated": status, "validated_at": now, "updated_at": now}}
	r.problemsCollection.UpdateOne(ctx, bson.M{"_id": problemUUID}, update)
	var problem model.Problem
	r.problemsCollection.FindOne(ctx, bson.M{"_id": problemUUID, "deleted_at": nil}).Decode(&problem)
	return problem.Validated
}

func (r *Repository) GetSubmissionsByOptionalProblemID(ctx context.Context, req *pb.GetSubmissionsRequest) (*pb.GetSubmissionsResponse, error) {
	var filter bson.M
	if req.ProblemId != nil && *req.ProblemId != "" {
			fmt.Println(req)
			id, err := primitive.ObjectIDFromHex(*req.ProblemId)
			if err != nil {
					return &pb.GetSubmissionsResponse{Success: false, Message: "invalid problem id: " + err.Error(), ErrorType: "INVALID_ID"}, nil
			}
			var problem struct{}
			err = r.problemsCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
			if err != nil {
					return &pb.GetSubmissionsResponse{Success: false, Submissions: []*pb.Submission{}, Message: "problem not found", ErrorType: "NOT_FOUND"}, nil
			}
			filter = bson.M{"problemId": *req.ProblemId}
	} else {
			filter = bson.M{}
	}

	if req.UserId != "" {
			filter["userId"] = req.UserId
	}

	limit := req.Limit
	if limit == 0 {
			limit = 10
	}
	page := req.Page
	if page == 0 {
			page = 1
	}
	skip := (page - 1) * limit

	cursor, err := r.submissionsCollection.Find(ctx, filter, &options.FindOptions{
			Skip:  func(i int32) *int64 { v := int64(i); return &v }(skip),
			Limit: func(i int32) *int64 { v := int64(i); return &v }(limit),
	})
	if err != nil {
			fmt.Println("error finding submissions:", err)
			return &pb.GetSubmissionsResponse{Success: false, Message: "failed to retrieve submissions", ErrorType: "DB_ERROR"}, nil
	}
	defer func() {
			if err := cursor.Close(ctx); err != nil {
					fmt.Println("error closing cursor:", err)
			}
	}()

	var submissions []model.Submission
	if err = cursor.All(ctx, &submissions); err != nil {
			fmt.Println("error decoding submissions:", err)
			return &pb.GetSubmissionsResponse{Success: false, Message: "failed to decode submissions", ErrorType: "DB_ERROR"}, nil
	}

	pbSubmissions := make([]*pb.Submission, len(submissions))
	for i, sub := range submissions {
			var challengeID string
			if sub.ChallengeID != nil {
					challengeID = *sub.ChallengeID
			}
			pbSubmissions[i] = &pb.Submission{
					Id:          sub.ID.Hex(),
					ProblemId:   sub.ProblemID,
					Title:       sub.Title,
					UserId:      sub.UserID,
					ChallengeId: challengeID,
					SubmittedAt: &pb.Timestamp{
							Seconds: sub.SubmittedAt.Unix(),
							Nanos:   int32(sub.SubmittedAt.Nanosecond()),
					},
					Score:         int32(sub.Score),
					Status:        sub.Status,
					Output:        sub.Output,
					Language:      sub.Language,
					ExecutionTime: float32(sub.ExecutionTime),
					Difficulty:    sub.Difficulty,
					IsFirst:       sub.IsFirst,
			}
	}


	return &pb.GetSubmissionsResponse{
			Submissions: pbSubmissions,
			Success:     true,
			Message:     "submissions retrieved successfully",
	}, nil
}

func (r *Repository) GetProblemByIDSlug(ctx context.Context, req *pb.GetProblemByIdSlugRequest) (*pb.GetProblemByIdSlugResponse, error) {
	var problem model.Problem
	filter := bson.M{"deleted_at": nil}
	if req.ProblemId != "" {
		id, err := primitive.ObjectIDFromHex(req.ProblemId)
		if err != nil {
			return &pb.GetProblemByIdSlugResponse{Message: "Invalid problem ID"}, nil
		}
		filter["_id"] = id
	} else if req.Slug != nil {
		filter["slug"] = *req.Slug
	}
	err := r.problemsCollection.FindOne(ctx, filter).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.GetProblemByIdSlugResponse{Message: "Problem not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.GetProblemByIdSlugResponse{
		Problemmetdata: ToProblemMetadataLite(problem),
		Message:        "Problem retrieved successfully",
	}, nil
}

func (r *Repository) GetProblemByIDList(ctx context.Context, req *pb.GetProblemByIdListRequest) (*pb.GetProblemByIdListResponse, error) {
	filter := bson.M{"deleted_at": nil}
	if len(req.Tags) > 0 {
		filter["tags"] = bson.M{"$all": req.Tags}
	}
	if req.Difficulty != "" {
		filter["difficulty"] = req.Difficulty
	}
	if req.SearchQuery != "" {
		filter["$or"] = []bson.M{
			{"title": bson.M{"$regex": req.SearchQuery, "$options": "i"}},
			{"description": bson.M{"$regex": req.SearchQuery, "$options": "i"}},
		}
	}
	opts := options.Find().SetSkip(int64(req.Page-1) * int64(req.PageSize)).SetLimit(int64(req.PageSize))
	cursor, err := r.problemsCollection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var problems []model.Problem
	if err = cursor.All(ctx, &problems); err != nil {
		return nil, err
	}
	resp := &pb.GetProblemByIdListResponse{
		Problemmetdata: make([]*pb.ProblemMetadataLite, len(problems)),
		Message:        "Problems retrieved successfully",
	}
	for i, p := range problems {
		resp.Problemmetdata[i] = ToProblemMetadataLite(p)
	}
	return resp, nil
}

// func (r *Repository) createIndividualSubmissionRecord(userId, problemId string, success bool, state string) {

// }

func ToProblem(p model.Problem) *pb.Problem {
	var deletedAt *pb.Timestamp
	if p.DeletedAt != nil {
		deletedAt = &pb.Timestamp{Seconds: p.DeletedAt.Unix()}
	}
	var validatedAt *pb.Timestamp
	if p.ValidatedAt != nil {
		validatedAt = &pb.Timestamp{Seconds: p.ValidatedAt.Unix()}
	}
	validateCode := make(map[string]*pb.ValidationCode)
	for lang, vc := range p.ValidateCode {
		validateCode[lang] = &pb.ValidationCode{
			Placeholder: vc.Placeholder,
			Code:        vc.Code,
			Template:    vc.Template,
		}
	}

	return &pb.Problem{
		ProblemId:          p.ID.Hex(),
		CreatedAt:          &pb.Timestamp{Seconds: p.CreatedAt.Unix(), Nanos: int32(p.CreatedAt.Nanosecond())},
		UpdatedAt:          &pb.Timestamp{Seconds: p.UpdatedAt.Unix(), Nanos: int32(p.UpdatedAt.Nanosecond())},
		DeletedAt:          deletedAt,
		Title:              p.Title,
		Description:        p.Description,
		Tags:               p.Tags,
		Testcases:          &pb.TestCases{Run: ToPBTestCases(p.TestCases.Run), Submit: ToPBTestCases(p.TestCases.Submit)},
		Difficulty:         p.Difficulty,
		SupportedLanguages: p.SupportedLanguages,
		ValidateCode:       validateCode,
		Validated:          p.Validated,
		ValidatedAt:        validatedAt,
	}
}

func ToProblemMetadata(p model.Problem) *pb.ProblemMetadata {
	return &pb.ProblemMetadata{
		ProblemId:          p.ID.Hex(),
		Title:              p.Title,
		Description:        p.Description,
		Tags:               p.Tags,
		TestcaseRun:        &pb.TestCaseRunOnly{Run: ToPBTestCases(p.TestCases.Run)},
		Difficulty:         p.Difficulty,
		SupportedLanguages: p.SupportedLanguages,
		Validated:          p.Validated,
	}
}

func ToProblemMetadataLite(p model.Problem) *pb.ProblemMetadataLite {
	placeholderMaps := make(map[string]string)
	for lang, vc := range p.ValidateCode {
		placeholderMaps[lang] = vc.Placeholder
	}
	return &pb.ProblemMetadataLite{
		ProblemId:          p.ID.Hex(),
		Title:              p.Title,
		Description:        p.Description,
		Tags:               p.Tags,
		TestcaseRun:        &pb.TestCaseRunOnly{Run: ToPBTestCases(p.TestCases.Run)},
		Difficulty:         p.Difficulty,
		SupportedLanguages: p.SupportedLanguages,
		Validated:          p.Validated,
		PlaceholderMaps:    placeholderMaps,
	}
}

func ToProblemResponse(p model.Problem) *pb.GetProblemResponse {
	return &pb.GetProblemResponse{Problem: ToProblem(p)}
}

func toTestCases(tcs []*pb.TestCase, existingIDs map[string]bool, isRun bool) []model.TestCase {
	result := make([]model.TestCase, 0, len(tcs))
	for _, tc := range tcs {
		id := tc.Id
		if id == "" {
			id = primitive.NewObjectID().Hex()
		}
		if existingIDs[id] {
			continue
		}
		result = append(result, model.TestCase{
			ID:       id,
			Input:    tc.Input,
			Expected: tc.Expected,
		})
	}
	return result
}

func ToPBTestCases(tcs []model.TestCase) []*pb.TestCase {
	result := make([]*pb.TestCase, len(tcs))
	for i, tc := range tcs {
		result[i] = &pb.TestCase{
			Id:       tc.ID,
			Input:    tc.Input,
			Expected: tc.Expected,
		}
	}
	return result
}

// func (r *Repository)GetSubmissions()[]

//	func convertTimestampToTime(pbTimestamp *pb.Timestamp) time.Time {
//		if pbTimestamp == nil {
//			return time.Time{}
//		}
//		return time.Unix(pbTimestamp.Seconds, int64(pbTimestamp.Nanos))
//	}
func (r *Repository) PushSubmissionData(ctx context.Context, submission *model.Submission, status string) {
	if r == nil || submission == nil {
		fmt.Println("repository or submission is nil")
		return
	}

	SuccessCount, err := r.submissionsCollection.CountDocuments(ctx, bson.M{
		"problemId": submission.ProblemID,
		"status":    "SUCCESS",
	})
	if err != nil {
		fmt.Println("failed to count successful submissions: %w", err)
	}

	// insert into submissions collection (all history)
	if SuccessCount == 0 && status == "SUCCESS"{
		submission.Score = CalculateScore(submission.Difficulty)
		submission.IsFirst =true
	}
	submissionObject, err := r.submissionsCollection.InsertOne(ctx, submission)
	if err != nil {
		fmt.Println("failed to insert into submissions:", err)
		return
	}

	// type assert InsertedID to primitive.ObjectID and convert to string
	submissionID, ok := submissionObject.InsertedID.(primitive.ObjectID)
	if !ok {
		fmt.Println("failed to assert submission ID to ObjectID")
		return
	}

	submissionIDHex := submissionID.Hex()
	fmt.Println("submission added:", submissionObject, "submission ID:", submissionIDHex)

	// check and insert into submission leaderboard entry if first successful submission
	if status == "SUCCESS" {
		if submission.ProblemID != "" {
			fmt.Println("SuccessCount is ", SuccessCount)

			if SuccessCount == 0 { 
				leaderboardEntry := model.ProblemDone{
					ID:           primitive.NewObjectID(),
					SubmissionID: submissionIDHex,
					ProblemID:    submission.ProblemID,
					UserID:       submission.UserID,
					Title:        submission.Title,
					Language:     submission.Language,
					Difficulty:   submission.Difficulty,
					SubmittedAt:  submission.SubmittedAt,
					Score:        CalculateScore(submission.Difficulty),
				}

				submission.IsFirst = true
				_, err = r.submissionFirstSuccessCollection.InsertOne(ctx, leaderboardEntry)
				if err != nil {
					fmt.Println("submission add error ", err)
				}
				fmt.Println("submission first added")
			}
		}
	}
}

func CalculateScore(difficulty string) int {
	score := 2

	switch difficulty {
	// case "EASY":
	// 	score = 2
	case "MEDIUM":
		score = 4
	case "HARD":
		score = 6
	}

	return score
}

func (r *Repository) ProblemsDoneStatistics(userID string) (model.ProblemsDoneStatistics, error) {
	if userID == "" {
			return model.ProblemsDoneStatistics{}, fmt.Errorf("userID cannot be empty")
	}

	// initialize the statistics structure
	stats := model.ProblemsDoneStatistics{
			MaxEasyCount:    0,
			DoneEasyCount:   0,
			MaxMediumCount:  0,
			DoneMediumCount: 0,
			MaxHardCount:    0,
			DoneHardCount:   0,
	}

	// get the total count of problems based on difficulty
	problemsCursor, err := r.problemsCollection.Find(context.TODO(), bson.M{"deleted_at": nil})
	if err != nil {
			fmt.Println("failed to fetch problems:", err)
			return stats, err
	}
	defer problemsCursor.Close(context.TODO())

	for problemsCursor.Next(context.TODO()) {
			var problem model.Problem
			if err := problemsCursor.Decode(&problem); err != nil {
					fmt.Println("failed to decode problem:", err)
					continue
			}

			switch problem.Difficulty {
			case "E":
					stats.MaxEasyCount++
			case "M":
					stats.MaxMediumCount++
			case "H":
					stats.MaxHardCount++
			}
	}

	// get the count of problems done by the user
	doneCursor, err := r.submissionFirstSuccessCollection.Find(context.TODO(), bson.M{"userId": userID})
	if err != nil {
			fmt.Println("failed to fetch submissions:", err)
			return stats, err
	}
	defer doneCursor.Close(context.TODO())

	for doneCursor.Next(context.TODO()) {
			var submission model.ProblemDone
			if err := doneCursor.Decode(&submission); err != nil {
					fmt.Println("failed to decode submission:", err)
					continue
			}

			switch submission.Difficulty {
			case "E":
					stats.DoneEasyCount++
			case "M":
					stats.DoneMediumCount++
			case "H":
					stats.DoneHardCount++
			}
	}

	// return the statistics
	return stats, nil
}