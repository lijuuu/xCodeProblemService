package repository

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
	"xcode/model"

	pb "github.com/lijuuu/GlobalProtoXcode/ProblemsService"
	redisboard "github.com/lijuuu/RedisBoard"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap/zapcore"

	zap_betterstack "xcode/logger"
)

type Repository struct {
	mongoclientInstance              *mongo.Client
	problemsCollection               *mongo.Collection
	challengeCollection              *mongo.Collection
	submissionsCollection            *mongo.Collection
	submissionFirstSuccessCollection *mongo.Collection
	lb                               *redisboard.Leaderboard

	logger *zap_betterstack.BetterStackLogStreamer
}

func NewRepository(client *mongo.Client, lb *redisboard.Leaderboard, logger *zap_betterstack.BetterStackLogStreamer) *Repository {
	return &Repository{
		mongoclientInstance:              client,
		problemsCollection:               client.Database("problems_db").Collection("problems"),
		submissionsCollection:            client.Database("submissions_db").Collection("submissions"),
		challengeCollection:              client.Database("challenges_db").Collection("challenges"),
		submissionFirstSuccessCollection: client.Database("submissions_db").Collection("submissionsfirstsuccess"),
		lb:                               lb,
		logger: logger,
	}
}

// SyncLeaderboardToRedis syncs MongoDB data to RedisBoard
func (r *Repository) SyncLeaderboardToRedis(ctx context.Context) error {

	syncStartTime := time.Now()
	r.logger.Log(zapcore.InfoLevel, "REDIBOARDSYNC", "Syncing Leaderboard to Redis started", nil, "REPOSITORY", nil)

	pipeline := mongo.Pipeline{
		// Sort by SubmittedAt to ensure consistent country selection
		{{Key: "$sort", Value: bson.M{"submittedAt": 1}}},
		// Group by UserID, sum Score, and take first Country
		{{Key: "$group", Value: bson.M{
			"_id":            "$userId",
			"totalScore":     bson.M{"$sum": "$score"},
			"primaryCountry": bson.M{"$first": "$country"},
		}}},
	}

	cursor, err := r.submissionFirstSuccessCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("failed to aggregate leaderboard data: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var result struct {
			ID             string `bson:"_id"`
			TotalScore     int    `bson:"totalScore"`
			PrimaryCountry string `bson:"primaryCountry"`
		}
		if err := cursor.Decode(&result); err != nil {
			return fmt.Errorf("failed to decode aggregation result: %w", err)
		}

		user := redisboard.User{
			ID:     result.ID,
			Entity: result.PrimaryCountry,
			Score:  float64(result.TotalScore),
		}

		// fmt.Println("adding ",user)
		if err := r.lb.AddUser(user); err != nil {
			return fmt.Errorf("failed to add user %s to RedisBoard: %w", result.ID, err)
		}
	}

	r.logger.Log(zapcore.InfoLevel, "REDIBOARDSYNC", "Syncing Leaderboard to Redis Finished", map[string]any{
		"duration": time.Since(syncStartTime).Seconds(),
	}, "REPOSITORY", nil)

	return cursor.Err()
}

// PushSubmissionData handles submission insertion and RedisBoard updates
func (r *Repository) PushSubmissionData(ctx context.Context, submission *model.Submission, status string) error {
	if r == nil || submission == nil {
		return fmt.Errorf("repository or submission is nil")
	}

	submission.Country = strings.ToUpper(submission.Country)

	// Count successful submissions for the problem
	SuccessCount, err := r.submissionsCollection.CountDocuments(ctx, bson.M{
		"userId":    submission.UserID,
		"problemId": submission.ProblemID,
		"status":    "SUCCESS",
	})
	if err != nil {
		return fmt.Errorf("failed to count successful submissions: %w", err)
	}

	// Insert into submissions collection (all history)
	if SuccessCount == 0 && status == "SUCCESS" {
		submission.Score = CalculateScore(submission.Difficulty)
		submission.IsFirst = true
	}
	submissionObject, err := r.submissionsCollection.InsertOne(ctx, submission)
	if err != nil {
		return fmt.Errorf("failed to insert into submissions: %w", err)
	}

	// Extract submission ID
	submissionID, ok := submissionObject.InsertedID.(primitive.ObjectID)
	if !ok {
		return fmt.Errorf("failed to assert submission ID to ObjectID")
	}
	submissionIDHex := submissionID.Hex()
	fmt.Println("submission added:", submissionIDHex)

	// Handle first successful submission
	if status == "SUCCESS" && submission.IsFirst {
		leaderboardEntry := model.ProblemDone{
			ID:           primitive.NewObjectID(),
			SubmissionID: submissionIDHex,
			ProblemID:    submission.ProblemID,
			UserID:       submission.UserID,
			Title:        submission.Title,
			Language:     submission.Language,
			Difficulty:   submission.Difficulty,
			SubmittedAt:  submission.SubmittedAt,
			Country:      submission.Country,
			Score:        submission.Score,
		}

		_, err = r.submissionFirstSuccessCollection.InsertOne(ctx, leaderboardEntry)
		if err != nil {
			return fmt.Errorf("failed to insert into submissionsfirstsuccess: %w", err)
		}
		fmt.Println("first successful submission added")

		// Update RedisBoard
		user := redisboard.User{
			ID:     submission.UserID,
			Entity: submission.Country,
			Score:  float64(submission.Score),
		}
		// Check if user exists in RedisBoard
		existingEntity, err := r.lb.GetUserEntity(submission.UserID)
		if err != nil || existingEntity == "" {
			// Add new user
			if err := r.lb.AddUser(user); err != nil {
				return fmt.Errorf("failed to add user %s to RedisBoard: %w", submission.UserID, err)
			}
		} else {
			// Increment score
			if err := r.lb.IncrementScore(submission.UserID, existingEntity, float64(submission.Score)); err != nil {
				return fmt.Errorf("failed to increment score for user %s: %w", submission.UserID, err)
			}
		}
	}
	return nil
}

// GetTopKGlobalMongo returns the top K users globally
func (r *Repository) GetTopKGlobalMongo(ctx context.Context, k int) ([]model.UserScore, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{
			"_id":            "$userId",
			"totalScore":     bson.M{"$sum": "$score"},
			"primaryCountry": bson.M{"$first": "$country"},
		}}},
		{{Key: "$sort", Value: bson.M{"totalScore": -1}}},
		{{Key: "$limit", Value: k}},
	}

	cursor, err := r.submissionFirstSuccessCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate top K global: %w", err)
	}
	defer cursor.Close(ctx)

	var results []model.UserScore
	for cursor.Next(ctx) {
		var result model.UserScore
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode top K global result: %w", err)
		}
		results = append(results, result)
	}
	return results, cursor.Err()
}

// GetTopKEntityMongo returns the top K users for a specific entity
func (r *Repository) GetTopKEntityMongo(ctx context.Context, entity string, k int) ([]model.UserScore, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"country": entity}}},
		{{Key: "$group", Value: bson.M{
			"_id":            "$userId",
			"totalScore":     bson.M{"$sum": "$score"},
			"primaryCountry": bson.M{"$first": "$country"},
		}}},
		{{Key: "$sort", Value: bson.M{"totalScore": -1}}},
		{{Key: "$limit", Value: k}},
	}

	cursor, err := r.submissionFirstSuccessCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate top K entity: %w", err)
	}
	defer cursor.Close(ctx)

	var results []model.UserScore
	for cursor.Next(ctx) {
		var result model.UserScore
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode top K entity result: %w", err)
		}
		results = append(results, result)
	}
	return results, cursor.Err()
}

// GetUserRankMongo returns the global and entity ranks for a user
func (r *Repository) GetUserRankMongo(ctx context.Context, userID string) (globalRank, entityRank int, err error) {
	// Global rank
	globalPipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{
			"_id":        "$userId",
			"totalScore": bson.M{"$sum": "$score"},
		}}},
		{{Key: "$sort", Value: bson.M{"totalScore": -1}}},
	}
	cursor, err := r.submissionFirstSuccessCollection.Aggregate(ctx, globalPipeline)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to aggregate global rank: %w", err)
	}
	defer cursor.Close(ctx)

	globalRank = 0
	foundGlobal := false
	for cursor.Next(ctx) {
		globalRank++
		var result struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&result); err != nil {
			return 0, 0, fmt.Errorf("failed to decode global rank result: %w", err)
		}
		if result.ID == userID {
			foundGlobal = true
			break
		}
	}
	if !foundGlobal {
		globalRank = 0
	}

	// Entity rank
	var userEntity string
	if err := r.submissionFirstSuccessCollection.FindOne(ctx, bson.M{"userId": userID}).Decode(&struct {
		Country string `bson:"country"`
	}{Country: userEntity}); err != nil {
		if err == mongo.ErrNoDocuments {
			return globalRank, 0, nil
		}
		return 0, 0, fmt.Errorf("failed to find user entity: %w", err)
	}

	entityPipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"country": userEntity}}},
		{{Key: "$group", Value: bson.M{
			"_id":        "$userId",
			"totalScore": bson.M{"$sum": "$score"},
		}}},
		{{Key: "$sort", Value: bson.M{"totalScore": -1}}},
	}
	cursor, err = r.submissionFirstSuccessCollection.Aggregate(ctx, entityPipeline)
	if err != nil {
		return globalRank, 0, fmt.Errorf("failed to aggregate entity rank: %w", err)
	}
	defer cursor.Close(ctx)

	entityRank = 0
	foundEntity := false
	for cursor.Next(ctx) {
		entityRank++
		var result struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&result); err != nil {
			return globalRank, 0, fmt.Errorf("failed to decode entity rank result: %w", err)
		}
		if result.ID == userID {
			foundEntity = true
			break
		}
	}
	if !foundEntity {
		entityRank = 0
	}

	return globalRank, entityRank, cursor.Err()
}

// GetLeaderboardDataMongo returns leaderboard data for a specific user
func (r *Repository) GetLeaderboardDataMongo(ctx context.Context, userID string) (*model.UserScore, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"userId": userID}}},
		{{Key: "$group", Value: bson.M{
			"_id":               "$userId",
			"totalScore":        bson.M{"$sum": "$score"},
			"primaryCountry":    bson.M{"$first": "$country"},
			"problemsDoneCount": bson.M{"$sum": 1},
		}}},
	}

	cursor, err := r.submissionFirstSuccessCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate leaderboard data: %w", err)
	}
	defer cursor.Close(ctx)

	if cursor.Next(ctx) {
		var result model.UserScore
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode leaderboard data: %w", err)
		}
		return &result, nil
	}
	return nil, mongo.ErrNoDocuments
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

func (r *Repository) GetProblemByIDList(ctx context.Context, req *pb.GetProblemMetadataListRequest) (*pb.GetProblemMetadataListResponse, error) {
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
	resp := &pb.GetProblemMetadataListResponse{
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

func (r *Repository) GetMonthlyContributionHistory(userID string, month, year int) (model.MonthlyActivityHeatmapProps, error) {
	// validate user id
	if userID == "" {
		return model.MonthlyActivityHeatmapProps{}, fmt.Errorf("userID cannot be empty")
	}

	// set default to current month and year if not provided
	var monthTime time.Month
	if month == 0 || year == 0 {
		now := time.Now()               // April 15, 2025
		year, monthTime, _ = now.Date() // Unpack into time.Month
		month = int(monthTime)          // Convert time.Month to int
	}

	// set date range for the entire month
	startDate := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	lastDay := time.Date(year, time.Month(month)+1, 0, 23, 59, 59, 999999999, time.UTC).Day()
	endDate := time.Date(year, time.Month(month), lastDay, 23, 59, 59, 999999999, time.UTC)

	// initialize baseline days for the full month
	var baseDays []model.ActivityDay
	for day := 1; day <= lastDay; day++ {
		dateStr := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
		baseDays = append(baseDays, model.ActivityDay{
			Date:     dateStr,
			Count:    0,
			IsActive: false,
		})
	}

	// aggregate submissions by day
	pipeline := []bson.M{
		{
			"$match": bson.M{
				"userId": userID,
				"submittedAt": bson.M{
					"$gte": startDate,
					"$lte": endDate,
				},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"$dateToString": bson.M{
						"format": "%Y-%m-%d",
						"date":   "$submittedAt",
					},
				},
				"count": bson.M{"$sum": 1},
			},
		},
		{
			"$project": bson.M{
				"_id":      0,
				"date":     "$_id",
				"count":    1,
				"isActive": bson.M{"$gt": []interface{}{"$count", 0}},
			},
		},
	}

	cursor, err := r.submissionsCollection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		fmt.Println("failed to aggregate submissions:", err)
		return model.MonthlyActivityHeatmapProps{}, err
	}
	defer cursor.Close(context.TODO())

	var activityDays []model.ActivityDay
	if err = cursor.All(context.TODO(), &activityDays); err != nil {
		fmt.Println("failed to decode activity days:", err)
		return model.MonthlyActivityHeatmapProps{}, err
	}

	// merge baseline with aggregated data
	dayMap := make(map[string]model.ActivityDay)
	for _, day := range baseDays {
		dayMap[day.Date] = day
	}
	for _, day := range activityDays {
		if existing, ok := dayMap[day.Date]; ok {
			existing.Count = day.Count
			existing.IsActive = day.IsActive
			dayMap[day.Date] = existing
		}
	}

	var finalDays []model.ActivityDay
	for _, day := range baseDays {
		finalDays = append(finalDays, dayMap[day.Date])
	}

	return model.MonthlyActivityHeatmapProps{
		Data: finalDays,
	}, nil
}

func (r *Repository) CreateChallenge(ctx context.Context, req *pb.CreateChallengeRequest, roomCode, password string) (*pb.CreateChallengeResponse, error) {
	count, err := r.challengeCollection.CountDocuments(ctx, bson.M{"title": req.Title, "deleted_at": nil})
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return nil, fmt.Errorf("challenge with this title already exists")
	}

	now := time.Now().Unix()
	challenge := model.Challenge{
		Title:               req.Title,
		CreatorID:           req.CreatorId,
		Difficulty:          req.Difficulty,
		IsPrivate:           req.IsPrivate,
		RoomCode:            roomCode,
		Password:            password,
		ProblemIDs:          req.ProblemIds,
		TimeLimit:           req.TimeLimit,
		CreatedAt:           now,
		IsActive:            true,
		ParticipantIDs:      []string{req.CreatorId},
		UserProblemMetadata: make(map[string][]model.ChallengeProblemMetadata),
		Status:              "CREATED",
	}

	res, err := r.challengeCollection.InsertOne(ctx, challenge)
	if err != nil {
		return nil, err
	}

	id := res.InsertedID.(primitive.ObjectID).Hex()
	return &pb.CreateChallengeResponse{
		Id:       id,
		Password: password,
		JoinUrl:  fmt.Sprintf("https://xcode.com/challenges/join/%s", id),
	}, nil
}

func (r *Repository) GetChallenge(ctx context.Context, req *pb.GetChallengeDetailsRequest) (*pb.GetChallengeDetailsResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.Id)
	if err != nil {
		return nil, err
	}

	var challenge model.Challenge
	err = r.challengeCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&challenge)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("challenge not found")
	}
	if err != nil {
		return nil, err
	}

	return &pb.GetChallengeDetailsResponse{
		Challenge:   ToPBChallenge(challenge),
		Leaderboard: nil,
	}, nil
}

func (r *Repository) GetChallengeDetails(ctx context.Context, req *pb.GetChallengeDetailsRequest) (*pb.GetChallengeDetailsResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.Id)
	if err != nil {
		return nil, err
	}

	var challenge model.Challenge
	err = r.challengeCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&challenge)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("challenge not found")
	}
	if err != nil {
		return nil, err
	}

	leaderboard, err := r.GetChallengeLeaderboard(ctx, req.Id)
	if err != nil {
		return nil, err
	}

	metadataList, exists := challenge.UserProblemMetadata[req.UserId]
	if !exists {
		metadataList = []model.ChallengeProblemMetadata{}
	}

	pbMetadata := make([]*pb.ChallengeProblemMetadata, len(metadataList))
	for i, m := range metadataList {
		pbMetadata[i] = ToPBChallengeProblemMetadata(m)
	}

	return &pb.GetChallengeDetailsResponse{
		Challenge:   ToPBChallenge(challenge),
		Leaderboard: leaderboard,
	}, nil
}

func (r *Repository) GetPublicChallenges(ctx context.Context, req *pb.GetPublicChallengesRequest) (*pb.GetPublicChallengesResponse, error) {
	filter := bson.M{
		"deleted_at": nil,
		"is_private": false,
	}

	if req.Difficulty != "" {
		filter["difficulty"] = req.Difficulty
	}
	if req.IsActive {
		filter["is_active"] = true
	}

	opts := options.Find().
		SetSkip(int64(req.Page-1) * int64(req.PageSize)).
		SetLimit(int64(req.PageSize))

	cursor, err := r.challengeCollection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find challenges: %w", err)
	}
	defer cursor.Close(ctx)

	var challenges []model.Challenge
	if err = cursor.All(ctx, &challenges); err != nil {
		return nil, fmt.Errorf("failed to decode challenges: %w", err)
	}

	pbChallenges := make([]*pb.Challenge, len(challenges))
	for i, c := range challenges {
		pbChallenges[i] = ToPBChallenge(c)
		pbChallenges[i].ProblemIds = nil
		pbChallenges[i].ParticipantIds = nil
		pbChallenges[i].UserProblemMetadata = nil
	}

	return &pb.GetPublicChallengesResponse{
		Challenges: pbChallenges,
	}, nil
}

func (r *Repository) JoinChallenge(ctx context.Context, req *pb.JoinChallengeRequest) (*pb.JoinChallengeResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ChallengeId)
	if err != nil {
		return nil, fmt.Errorf("invalid challenge_id")
	}

	var challenge model.Challenge
	err = r.challengeCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&challenge)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("challenge not found")
	}
	if err != nil {
		return nil, err
	}

	if challenge.IsPrivate && (req.Password == nil || *req.Password != challenge.Password) {
		return &pb.JoinChallengeResponse{
			ChallengeId: req.ChallengeId,
			Success:     false,
			Message:     "Invalid password",
		}, nil
	}

	if slices.Contains(challenge.ParticipantIDs, req.UserId) {
		return &pb.JoinChallengeResponse{
			ChallengeId: req.ChallengeId,
			Success:     true,
			Message:     "Already joined",
		}, nil
	}

	update := bson.M{
		"$push": bson.M{"participant_ids": req.UserId},
		"$set":  bson.M{"updated_at": time.Now().Unix()},
	}

	result, err := r.challengeCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return nil, fmt.Errorf("challenge not found")
	}

	return &pb.JoinChallengeResponse{
		ChallengeId: req.ChallengeId,
		Success:     true,
		Message:     "Joined successfully",
	}, nil
}

func (r *Repository) StartChallenge(ctx context.Context, req *pb.StartChallengeRequest) (*pb.StartChallengeResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ChallengeId)
	if err != nil {
		return nil, err
	}

	var challenge model.Challenge
	err = r.challengeCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&challenge)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("challenge not found")
	}
	if err != nil {
		return nil, err
	}

	if challenge.CreatorID != req.UserId {
		return nil, fmt.Errorf("only creator can start challenge")
	}
	if challenge.IsActive {
		return nil, fmt.Errorf("challenge already active")
	}

	startTime := time.Now().Unix()
	update := bson.M{
		"$set": bson.M{
			"is_active":  true,
			"start_time": startTime,
			"status":     "ACTIVE",
			"updated_at": time.Now().Unix(),
			"end_time":   startTime + int64(challenge.TimeLimit)*60,
		},
	}

	result, err := r.challengeCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return nil, fmt.Errorf("challenge not found")
	}

	return &pb.StartChallengeResponse{
		Success:   true,
		StartTime: startTime,
	}, nil
}

func (r *Repository) EndChallenge(ctx context.Context, req *pb.EndChallengeRequest) (*pb.EndChallengeResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ChallengeId)
	if err != nil {
		return nil, err
	}

	var challenge model.Challenge
	err = r.challengeCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&challenge)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("challenge not found")
	}
	if err != nil {
		return nil, err
	}

	if challenge.CreatorID != req.UserId {
		return nil, fmt.Errorf("only creator can end challenge")
	}
	if !challenge.IsActive {
		return nil, fmt.Errorf("challenge not active")
	}

	update := bson.M{
		"$set": bson.M{
			"is_active":  false,
			"status":     "COMPLETED",
			"end_time":   time.Now().Unix(),
			"updated_at": time.Now().Unix(),
		},
	}

	result, err := r.challengeCollection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return nil, err
	}
	if result.MatchedCount == 0 {
		return nil, fmt.Errorf("challenge not found")
	}

	return &pb.EndChallengeResponse{
		Success: true,
	}, nil
}

func (r *Repository) GetSubmissionStatus(ctx context.Context, req *pb.GetSubmissionStatusRequest) (*pb.GetSubmissionStatusResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.SubmissionId)
	if err != nil {
		return nil, err
	}

	var submission model.Submission
	err = r.submissionsCollection.FindOne(ctx, bson.M{"_id": id}).Decode(&submission)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("submission not found")
	}
	if err != nil {
		return nil, err
	}

	return &pb.GetSubmissionStatusResponse{
		Submission: ToPBSubmission(submission),
	}, nil
}

func (r *Repository) GetChallengeSubmissions(ctx context.Context, req *pb.GetChallengeSubmissionsRequest) (*pb.GetChallengeSubmissionsResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ChallengeId)
	if err != nil {
		return nil, err
	}

	var challenge struct{}
	err = r.challengeCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&challenge)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("challenge not found")
	}
	if err != nil {
		return nil, err
	}

	cursor, err := r.submissionsCollection.Find(ctx, bson.M{"challenge_id": req.ChallengeId})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var submissions []model.Submission
	if err = cursor.All(ctx, &submissions); err != nil {
		return nil, err
	}

	pbSubmissions := make([]*pb.Submission, len(submissions))
	for i, s := range submissions {
		pbSubmissions[i] = ToPBSubmission(s)
	}

	return &pb.GetChallengeSubmissionsResponse{
		Submissions: pbSubmissions,
	}, nil
}

func (r *Repository) GetChallengeLeaderboard(ctx context.Context, challengeID string) ([]*pb.LeaderboardEntry, error) {
	id, err := primitive.ObjectIDFromHex(challengeID)
	if err != nil {
		return nil, err
	}

	var challenge model.Challenge
	err = r.challengeCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&challenge)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("challenge not found")
	}
	if err != nil {
		return nil, err
	}

	leaderboard := make([]*pb.LeaderboardEntry, 0)
	for userID, metadataList := range challenge.UserProblemMetadata {
		totalScore := 0
		problemsCompleted := 0
		for _, metadata := range metadataList {
			totalScore += metadata.Score
			problemsCompleted++
		}

		leaderboard = append(leaderboard, &pb.LeaderboardEntry{
			UserId:            userID,
			ProblemsCompleted: int32(problemsCompleted),
			TotalScore:        int32(totalScore),
		})
	}

	for i := 0; i < len(leaderboard)-1; i++ {
		for j := i + 1; j < len(leaderboard); j++ {
			if leaderboard[i].TotalScore < leaderboard[j].TotalScore {
				leaderboard[i], leaderboard[j] = leaderboard[j], leaderboard[i]
			}
		}
	}

	for i, entry := range leaderboard {
		entry.Rank = int32(i + 1)
	}

	return leaderboard, nil
}

func (r *Repository) GetUserStats(ctx context.Context, req *pb.GetUserStatsRequest) (*pb.GetUserStatsResponse, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"participant_ids": req.UserId, "deleted_at": nil}}},
		{{Key: "$unwind", Value: bson.M{"path": "$user_problem_metadata." + req.UserId, "preserveNullAndEmptyArrays": true}}},
		{{Key: "$group", Value: bson.M{
			"_id":                "$_id",
			"problems_completed": bson.M{"$sum": 1},
			"total_score":        bson.M{"$sum": "$user_problem_metadata." + req.UserId + ".score"},
			"total_time_taken":   bson.M{"$sum": "$user_problem_metadata." + req.UserId + ".time_taken"},
		}}},
	}

	cursor, err := r.challengeCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var stats model.UserStats
	challengesCompleted := 0
	challengeStats := make(map[string]*pb.ChallengeStat)
	for cursor.Next(ctx) {
		var result struct {
			ProblemsCompleted int32 `bson:"problems_completed"`
			TotalScore        int32 `bson:"total_score"`
			TotalTimeTaken    int64 `bson:"total_time_taken"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		stats.ProblemsCompleted += result.ProblemsCompleted
		stats.TotalTimeTaken += result.TotalTimeTaken
		stats.Score += float64(result.TotalScore)
		challengesCompleted++

		leaderboard, err := r.GetChallengeLeaderboard(ctx, req.UserId)
		if err != nil {
			continue
		}
		rank := int32(0)
		for _, entry := range leaderboard {
			if entry.UserId == req.UserId {
				rank = entry.Rank
				break
			}
		}
		challengeStats[req.UserId] = &pb.ChallengeStat{
			Rank:              rank,
			ProblemsCompleted: result.ProblemsCompleted,
			TotalScore:        result.TotalScore,
		}
	}

	return &pb.GetUserStatsResponse{
		Stats: &pb.UserStats{
			UserId:              req.UserId,
			ProblemsCompleted:   stats.ProblemsCompleted,
			TotalTimeTaken:      stats.TotalTimeTaken,
			ChallengesCompleted: int32(challengesCompleted),
			Score:               stats.Score,
			ChallengeStats:      challengeStats,
		},
	}, nil
}

func (r *Repository) GetChallengeUserStats(ctx context.Context, req *pb.GetChallengeUserStatsRequest) (*pb.GetChallengeUserStatsResponse, error) {
	id, err := primitive.ObjectIDFromHex(req.ChallengeId)
	if err != nil {
		return nil, err
	}

	var challenge model.Challenge
	err = r.challengeCollection.FindOne(ctx, bson.M{"_id": id, "deleted_at": nil}).Decode(&challenge)
	if err == mongo.ErrNoDocuments {
		return nil, fmt.Errorf("challenge not found")
	}
	if err != nil {
		return nil, err
	}

	metadataList, exists := challenge.UserProblemMetadata[req.UserId]
	if !exists {
		return &pb.GetChallengeUserStatsResponse{
			UserId:                   req.UserId,
			ProblemsCompleted:        0,
			TotalScore:               0,
			Rank:                     0,
			ChallengeProblemMetadata: nil,
		}, nil
	}

	problemsCompleted := 0
	totalScore := 0
	pbMetadata := make([]*pb.ChallengeProblemMetadata, len(metadataList))
	for i, m := range metadataList {
		problemsCompleted++
		totalScore += m.Score
		pbMetadata[i] = ToPBChallengeProblemMetadata(m)
	}

	leaderboard, err := r.GetChallengeLeaderboard(ctx, req.ChallengeId)
	if err != nil {
		return nil, err
	}

	rank := int32(0)
	for _, entry := range leaderboard {
		if entry.UserId == req.UserId {
			rank = entry.Rank
			break
		}
	}

	return &pb.GetChallengeUserStatsResponse{
		UserId:                   req.UserId,
		ProblemsCompleted:        int32(problemsCompleted),
		TotalScore:               int32(totalScore),
		Rank:                     rank,
		ChallengeProblemMetadata: pbMetadata,
	}, nil
}

func ToPBChallenge(c model.Challenge) *pb.Challenge {
	userProblemMetadata := make(map[string]*pb.ProblemMetadataList)
	for userID, metadataList := range c.UserProblemMetadata {
		pbMetadata := make([]*pb.ChallengeProblemMetadata, len(metadataList))
		for i, m := range metadataList {
			pbMetadata[i] = ToPBChallengeProblemMetadata(m)
		}
		userProblemMetadata[userID] = &pb.ProblemMetadataList{ChallengeProblemMetadata: pbMetadata}
	}

	return &pb.Challenge{
		Id:                  c.ID.Hex(),
		Title:               c.Title,
		CreatorId:           c.CreatorID,
		Difficulty:          c.Difficulty,
		IsPrivate:           c.IsPrivate,
		Password:            &c.Password,
		ProblemIds:          c.ProblemIDs,
		TimeLimit:           c.TimeLimit,
		CreatedAt:           c.CreatedAt,
		IsActive:            c.IsActive,
		ParticipantIds:      c.ParticipantIDs,
		UserProblemMetadata: userProblemMetadata,
		Status:              c.Status,
		StartTime:           c.StartTime,
		EndTime:             c.EndTime,
	}
}

func ToPBChallengeProblemMetadata(m model.ChallengeProblemMetadata) *pb.ChallengeProblemMetadata {
	return &pb.ChallengeProblemMetadata{
		ProblemId:   m.ProblemID,
		Score:       int32(m.Score),
		TimeTaken:   m.TimeTaken,
		CompletedAt: m.CompletedAt,
	}
}

func ToPBSubmission(s model.Submission) *pb.Submission {
	var challengeID string
	if s.ChallengeID != nil {
		challengeID = *s.ChallengeID
	}
	return &pb.Submission{
		Id:          s.ID.Hex(),
		ProblemId:   s.ProblemID,
		Title:       s.Title,
		UserId:      s.UserID,
		ChallengeId: challengeID,
		SubmittedAt: &pb.Timestamp{
			Seconds: s.SubmittedAt.Unix(),
			Nanos:   int32(s.SubmittedAt.Nanosecond()),
		},
		Score:         int32(s.Score),
		Status:        s.Status,
		Output:        s.Output,
		Language:      s.Language,
		ExecutionTime: float32(s.ExecutionTime),
		Difficulty:    s.Difficulty,
		IsFirst:       s.IsFirst,
	}
}

// GetChallengeHistory retrieves a paginated list of challenges for a user
func (r *Repository) GetChallengeHistory(ctx context.Context, req *pb.GetChallengeHistoryRequest) (*pb.GetChallengeHistoryResponse, error) {
	// Build filter for challenges where user is a participant
	filter := bson.M{
		"participant_ids": req.UserId,
		"deleted_at":      nil,
		"is_private":      req.IsPrivate,
	}

	// fmt.Println(req)

	// Pagination options
	opts := options.Find().
		SetSkip(int64(req.Page-1) * int64(req.PageSize)).
		SetLimit(int64(req.PageSize)).
		SetSort(bson.M{"created_at": -1}) // Sort by creation date, newest first

	// Query challenges
	cursor, err := r.challengeCollection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find challenges: %w", err)
	}
	defer cursor.Close(ctx)

	// Decode challenges
	var challenges []model.Challenge
	if err = cursor.All(ctx, &challenges); err != nil {
		return nil, fmt.Errorf("failed to decode challenges: %w", err)
	}

	// Count total matching documents
	total, err := r.challengeCollection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to count challenges: %w", err)
	}

	// Convert to protobuf
	pbChallenges := make([]*pb.Challenge, len(challenges))
	for i, c := range challenges {
		pbChallenges[i] = ToPBChallenge(c)
	}

	return &pb.GetChallengeHistoryResponse{
		Challenges: pbChallenges,
		TotalCount: int32(total),
		Page:       req.Page,
		PageSize:   req.PageSize,
	}, nil
}

func (r *Repository) ForceChangeUserCountryInSubmission(ctx context.Context, req *pb.ForceChangeUserEntityInSubmissionRequest) {
	newEntity := strings.ToUpper(req.Entity)

	filter := bson.M{"userId": req.UserId}
	update := bson.M{
		"$set": bson.M{
			"country": newEntity,
		},
	}

	const maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		_, err := r.submissionFirstSuccessCollection.UpdateMany(ctx, filter, update)
		if err == nil {
			return
		}
		time.Sleep(time.Millisecond * 50) // simple backoff
	}
}
