package repository

import (
	"context"
	"time"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Repository struct {
	mongoclientInstance *mongo.Client
	collection          *mongo.Collection
}

func NewRepository(client *mongo.Client) *Repository {
	return &Repository{
		mongoclientInstance: client,
		collection:          client.Database("problems_db").Collection("problems"),
	}
}

func (r *Repository) CreateProblem(ctx context.Context, req *pb.CreateProblemRequest) (*pb.CreateProblemResponse, error) {
	now := time.Now()
	problem := bson.M{
		"title":               req.Title,
		"description":         req.Description,
		"tags":                req.Tags,
		"difficulty":          req.Difficulty,
		"created_at":          now,
		"updated_at":          now,
		"deleted_at":          nil,
		"testcases":           bson.M{"run": []any{}, "submit": []any{}},
		"supported_languages": []string{},
		"validate_code":       bson.M{},
		"validated":           false,
	}
	res, err := r.collection.InsertOne(ctx, problem)
	if err != nil {
		return &pb.CreateProblemResponse{Success: false, Message: err.Error()}, err
	}
	id := res.InsertedID.(primitive.ObjectID).Hex()
	return &pb.CreateProblemResponse{ProblemId: id, Success: true, Message: "Problem created"}, nil
}

func (r *Repository) UpdateProblem(ctx context.Context, req *pb.UpdateProblemRequest) (*pb.UpdateProblemResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.UpdateProblemResponse{Success: false, Message: "Invalid problem ID"}, err
	}
	update := bson.M{"$set": bson.M{"updated_at": time.Now()}}
	if req.Title != nil {
		update["$set"].(bson.M)["title"] = req.Title
	}
	if req.Description != nil {
		update["$set"].(bson.M)["description"] = req.Description
	}
	if len(req.Tags) > 0 {
		update["$set"].(bson.M)["tags"] = req.Tags
	}
	if req.Difficulty != nil {
		update["$set"].(bson.M)["difficulty"] = req.Difficulty
	}
	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return &pb.UpdateProblemResponse{Success: false, Message: err.Error()}, err
	}
	if result.MatchedCount == 0 {
		return &pb.UpdateProblemResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.UpdateProblemResponse{Success: true, Message: "Problem updated"}, nil
}

func (r *Repository) DeleteProblem(ctx context.Context, req *pb.DeleteProblemRequest) (*pb.DeleteProblemResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.DeleteProblemResponse{Success: false, Message: "Invalid problem ID"}, err
	}
	update := bson.M{"$set": bson.M{"deleted_at": time.Now()}}
	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return &pb.DeleteProblemResponse{Success: false, Message: err.Error()}, err
	}
	if result.MatchedCount == 0 {
		return &pb.DeleteProblemResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.DeleteProblemResponse{Success: true, Message: "Problem marked as deleted"}, nil
}

func (r *Repository) GetProblem(ctx context.Context, req *pb.GetProblemRequest) (*pb.GetProblemResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.GetProblemResponse{}, err
	}
	var problem bson.M
	err = r.collection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.GetProblemResponse{}, nil
	}
	if err != nil {
		return &pb.GetProblemResponse{}, err
	}
	return r.toProblemResponse(problem), nil
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
	opts := options.Find().
		SetSkip(int64(req.Page-1) * int64(req.PageSize)).
		SetLimit(int64(req.PageSize))
	cursor, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return &pb.ListProblemsResponse{}, err
	}
	defer cursor.Close(ctx)
	var problems []bson.M
	if err = cursor.All(ctx, &problems); err != nil {
		return &pb.ListProblemsResponse{}, err
	}
	total, err := r.collection.CountDocuments(ctx, filter)
	if err != nil {
		return &pb.ListProblemsResponse{}, err
	}
	resp := &pb.ListProblemsResponse{
		Problems:   make([]*pb.Problem, len(problems)),
		TotalCount: int32(total),
		Page:       req.Page,
		PageSize:   req.PageSize,
	}
	for i, p := range problems {
		resp.Problems[i] = r.toProblem(p)
	}
	return resp, nil
}

func (r *Repository) AddTestCases(ctx context.Context, req *pb.AddTestCasesRequest) (*pb.AddTestCasesResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.AddTestCasesResponse{Success: false, Message: "Invalid problem ID"}, err
	}
	update := bson.M{
		"$push": bson.M{
			"testcases.run":    bson.M{"$each": r.toTestCases(req.Testcases.Run)},
			"testcases.submit": bson.M{"$each": r.toTestCases(req.Testcases.Submit)},
		},
		"$set": bson.M{"updated_at": time.Now()},
	}
	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return &pb.AddTestCasesResponse{Success: false, Message: err.Error()}, err
	}
	if result.MatchedCount == 0 {
		return &pb.AddTestCasesResponse{Success: false, Message: "Problem not found"}, nil
	}
	count := len(req.Testcases.Run) + len(req.Testcases.Submit)
	return &pb.AddTestCasesResponse{Success: true, Message: "Test cases added", AddedCount: int32(count)}, nil
}

func (r *Repository) RemoveTestCases(ctx context.Context, req *pb.RemoveTestCasesRequest) (*pb.RemoveTestCasesResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.RemoveTestCasesResponse{Success: false, Message: "Invalid problem ID"}, err
	}
	update := bson.M{"$set": bson.M{"updated_at": time.Now()}}
	if len(req.RunIndices) > 0 {
		update["$pull"] = bson.M{"testcases.run": bson.M{"$in": req.RunIndices}}
	}
	if len(req.SubmitIndices) > 0 {
		update["$pull"] = bson.M{"testcases.submit": bson.M{"$in": req.SubmitIndices}}
	}
	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return &pb.RemoveTestCasesResponse{Success: false, Message: err.Error()}, err
	}
	if result.MatchedCount == 0 {
		return &pb.RemoveTestCasesResponse{Success: false, Message: "Problem not found"}, nil
	}
	count := len(req.RunIndices) + len(req.SubmitIndices)
	return &pb.RemoveTestCasesResponse{Success: true, Message: "Test cases removed", RemovedCount: int32(count)}, nil
}

func (r *Repository) AddLanguageSupport(ctx context.Context, req *pb.AddLanguageSupportRequest) (*pb.AddLanguageSupportResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.AddLanguageSupportResponse{Success: false, Message: "Invalid problem ID"}, err
	}
	update := bson.M{
		"$push": bson.M{"supported_languages": req.Language},
		"$set": bson.M{
			"validate_code." + req.Language: bson.M{
				"placeholder": req.ValidationCode.Placeholder,
				"code":        req.ValidationCode.Code,
			},
			"updated_at": time.Now(),
		},
	}
	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return &pb.AddLanguageSupportResponse{Success: false, Message: err.Error()}, err
	}
	if result.MatchedCount == 0 {
		return &pb.AddLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.AddLanguageSupportResponse{Success: true, Message: "Language support added"}, nil
}

func (r *Repository) UpdateLanguageSupport(ctx context.Context, req *pb.UpdateLanguageSupportRequest) (*pb.UpdateLanguageSupportResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.UpdateLanguageSupportResponse{Success: false, Message: "Invalid problem ID"}, err
	}
	update := bson.M{
		"$set": bson.M{
			"validate_code." + req.Language: bson.M{
				"placeholder": req.Validate.Placeholder,
				"code":        req.Validate.Code,
			},
			"updated_at": time.Now(),
		},
	}
	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return &pb.UpdateLanguageSupportResponse{Success: false, Message: err.Error()}, err
	}
	if result.MatchedCount == 0 {
		return &pb.UpdateLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.UpdateLanguageSupportResponse{Success: true, Message: "Language support updated"}, nil
}

func (r *Repository) RemoveLanguageSupport(ctx context.Context, req *pb.RemoveLanguageSupportRequest) (*pb.RemoveLanguageSupportResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.RemoveLanguageSupportResponse{Success: false, Message: "Invalid problem ID"}, err
	}
	update := bson.M{
		"$pull":  bson.M{"supported_languages": req.Language},
		"$unset": bson.M{"validate_code." + req.Language: ""},
		"$set":   bson.M{"updated_at": time.Now()},
	}
	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return &pb.RemoveLanguageSupportResponse{Success: false, Message: err.Error()}, err
	}
	if result.MatchedCount == 0 {
		return &pb.RemoveLanguageSupportResponse{Success: false, Message: "Problem not found"}, nil
	}
	return &pb.RemoveLanguageSupportResponse{Success: true, Message: "Language support removed"}, nil
}

func (r *Repository) FullValidationByProblemID(ctx context.Context, req *pb.FullValidationByProblemIDRequest) (*pb.FullValidationByProblemIDResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ProblemId)
	if err != nil {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Invalid problem ID", ErrorType: "INVALID_ID"}, err
	}
	var problem bson.M
	err = r.collection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&problem)
	if err == mongo.ErrNoDocuments {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: "Problem not found", ErrorType: "NOT_FOUND"}, nil
	}
	if err != nil {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: err.Error(), ErrorType: "DATABASE_ERROR"}, err
	}
	validated := true // Placeholder for actual validation logic
	update := bson.M{"$set": bson.M{"validated": validated, "validated_at": time.Now(), "updated_at": time.Now()}}
	_, err = r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return &pb.FullValidationByProblemIDResponse{Success: false, Message: err.Error(), ErrorType: "DATABASE_ERROR"}, err
	}
	return &pb.FullValidationByProblemIDResponse{Success: true, Message: "Validation completed"}, nil
}

// Helper functions
func (r *Repository) toProblem(p bson.M) *pb.Problem {
	id := p["_id"].(primitive.ObjectID).Hex()
	createdAt := p["created_at"].(primitive.DateTime).Time()
	updatedAt := p["updated_at"].(primitive.DateTime).Time()
	var deletedAt *pb.Timestamp
	if p["deleted_at"] != nil {
		dt := p["deleted_at"].(primitive.DateTime).Time()
		deletedAt = &pb.Timestamp{Seconds: dt.Unix(), Nanos: int32(dt.Nanosecond())}
	}
	testcases := p["testcases"].(bson.M)
	run := r.toPBTestCases(testcases["run"].(primitive.A))
	submit := r.toPBTestCases(testcases["submit"].(primitive.A))
	validateCode := make(map[string]*pb.ValidationCode)
	for lang, v := range p["validate_code"].(bson.M) {
		vc := v.(bson.M)
		validateCode[lang] = &pb.ValidationCode{
			Placeholder: vc["placeholder"].(string),
			Code:        vc["code"].(string),
		}
	}

	var validatedAt *pb.Timestamp
	if p["validated_at"] != nil {
		vt := p["validated_at"].(primitive.DateTime).Time()
		validatedAt = &pb.Timestamp{Seconds: vt.Unix(), Nanos: int32(vt.Nanosecond())}
	}

	return &pb.Problem{
		ProblemId:          id,
		CreatedAt:          &pb.Timestamp{Seconds: createdAt.Unix(), Nanos: int32(createdAt.Nanosecond())},
		UpdatedAt:          &pb.Timestamp{Seconds: updatedAt.Unix(), Nanos: int32(updatedAt.Nanosecond())},
		DeletedAt:          deletedAt,
		Title:              p["title"].(string),
		Description:        p["description"].(string),
		Tags:               r.toStringSlice(p["tags"].(primitive.A)),
		Testcases:          &pb.TestCases{Run: run, Submit: submit},
		Difficulty:         p["difficulty"].(string),
		SupportedLanguages: r.toStringSlice(p["supported_languages"].(primitive.A)),
		ValidateCode:       validateCode,
		Validated:          p["validated"].(bool),
		ValidatedAt:        validatedAt,
	}
}

func (r *Repository) toProblemResponse(p bson.M) *pb.GetProblemResponse {
	return &pb.GetProblemResponse{Problem: r.toProblem(p)}
}

func (r *Repository) toTestCases(tcs []*pb.TestCase) []any {
	result := make([]any, len(tcs))
	for i, tc := range tcs {
		result[i] = bson.M{"input": tc.Input, "expected": tc.Expected}
	}
	return result
}

func (r *Repository) toPBTestCases(tcs primitive.A) []*pb.TestCase {
	result := make([]*pb.TestCase, len(tcs))
	for i, tc := range tcs {
		t := tc.(bson.M)
		result[i] = &pb.TestCase{Input: t["input"].(string), Expected: t["expected"].(string)}
	}
	return result
}

func (r *Repository) toStringSlice(a primitive.A) []string {
	result := make([]string, len(a))
	for i, v := range a {
		result[i] = v.(string)
	}
	return result
}
