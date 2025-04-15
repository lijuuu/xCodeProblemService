package model

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type GenericResponse struct {
	Success bool        `json:"success"`
	Status  int         `json:"status"`
	Payload interface{} `json:"payload,omitempty"`
	Error   *ErrorInfo  `json:"error,omitempty"`
}

type ErrorInfo struct {
	ErrorType string `json:"errorType"`
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Details   string `json:"details,omitempty"`
}

type Problem struct {
	ID                 primitive.ObjectID  `bson:"_id,omitempty"`
	Title              string              `bson:"title"`
	Description        string              `bson:"description"`
	Tags               []string            `bson:"tags"`
	Difficulty         string              `bson:"difficulty"`
	CreatedAt          time.Time           `bson:"created_at"`
	UpdatedAt          time.Time           `bson:"updated_at"`
	DeletedAt          *time.Time          `bson:"deleted_at,omitempty"`
	TestCases          TestCaseCollection  `bson:"testcases"`
	SupportedLanguages []string            `bson:"supported_languages"`
	ValidateCode       map[string]CodeData `bson:"validate_code"`
	Validated          bool                `bson:"validated"`
	ValidatedAt        *time.Time          `bson:"validated_at,omitempty"`
}

type ProblemDone struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	SubmissionID string             `bson:"submissionId" json:"submissionId"`
	ProblemID    string             `bson:"problemId" json:"problemId"`
	UserID       string             `bson:"userId" json:"userId"`
	Title        string             `bson:"title" json:"title"`
	Language     string             `bson:"language" json:"language"`
	Difficulty   string             `bson:"difficulty" json:"difficulty"`
	SubmittedAt  time.Time          `bson:"submittedAt" json:"submittedAt"`
	Country      string             `bson:"country"`
	Score        int                `json:"score" bson:"score"`
}

type Submission struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID        string             `bson:"userId" json:"userId"`
	ProblemID     string             `bson:"problemId" json:"problemId"`
	ChallengeID   *string            `bson:"challengeId,omitempty" json:"challengeId,omitempty"`
	Title         string             `bson:"title" json:"title"`
	SubmittedAt   time.Time          `bson:"submittedAt" json:"submittedAt"`
	Status        string             `bson:"status" json:"status"`
	Score         int                `bson:"score" json:"score"`
	Language      string             `bson:"language" json:"language"`
	UserCode      string             `bson:"userCode" json:"userCode"`
	Output        string             `bson:"output,omitempty" json:"output,omitempty"`
	ExecutionTime float64            `bson:"executionTime,omitempty" json:"executionTime,omitempty"`
	Difficulty    string             `bson:"difficulty" json:"difficulty"`
	IsFirst       bool               `bson:"isFirst" json:"isFirst"`
}

type CodeData struct {
	Placeholder string `bson:"placeholder"`
	Code        string `bson:"code"`
	Template    string `bson:"template"`
}

type TestCase struct {
	ID       string `bson:"id"`
	Input    string `bson:"input"`
	Expected string `bson:"expected"`
}

type TestCaseCollection struct {
	Run    []TestCase `bson:"run"`
	Submit []TestCase `bson:"submit"`
}

// (alias) type ExecutionResult = {
// 	totalTestCases: number;
// 	passedTestCases: number;
// 	failedTestCases: number;
// 	overallPass: boolean;
// 	failedTestCase?: TestResult;
// 	syntaxError?: string;

type ExecutionStatsResult struct {
	TotalTestCases  int  `json:"totalTestCases"`
	PassedTestCases int  `json:"passedTestCases"`
	FailedTestCases int  `json:"failedTestCases"`
	OverallPass     bool `json:"overallPass"`
}

type ExecutionResult struct {
	ExecutionStatsResult
	FailedTestCase FailedTestCase `json:"failedTestCase"`
}

type FailedTestCase struct {
	TestCaseIndex int  `json:"testCaseIndex"`
	Input         any  `json:"input"`
	Expected      any  `json:"expected"`
	Received      any  `json:"received"`
	Passed        bool `json:"passed"`
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

type GetLeaderBoardOptionalCountryResponse struct {
	Data       LeaderboardSingle `json:"data" bson:"data"`
	FilterType *string           `json:"filterType,omitempty" bson:"filterType,omitempty"` // country or global
}

type LeaderboardSingle struct {
	Username    string `json:"username" bson:"username"`
	CountryRank *int64 `json:"countryRank,omitempty" bson:"countryRank,omitempty"`
	GlobalRank  int64  `json:"globalRank" bson:"globalRank"`
}

type GetLeaderBoardOptionalCountryRequest struct {
	Page     int64   `json:"page" bson:"page"`
	Limit    int64   `json:"limit" bson:"limit"`
	Country  *string `json:"country,omitempty" bson:"country,omitempty"`
	Username *string `json:"username,omitempty" bson:"username,omitempty"`
	UserID   string  `json:"userId" bson:"userId"`
}

type GetProblemsDoneStatisticsRequest struct {
	Username *string `json:"username,omitempty" bson:"username,omitempty"`
	UserID   string  `json:"userId" bson:"userId"`
}

type GetUserRankRequest struct {
	Username *string `json:"username,omitempty" bson:"username,omitempty"`
	UserID   string  `json:"userId" bson:"userId"`
}

type GetProblemsDoneStatisticsResponse struct {
	Data ProblemsDoneStatistics `json:"data" bson:"data"`
}

type ProblemsDoneStatistics struct {
	MaxEasyCount    int32 `json:"maxEasyCount" bson:"maxEasyCount"`
	DoneEasyCount   int32 `json:"doneEasyCount" bson:"doneEasyCount"`
	MaxMediumCount  int32 `json:"maxMediumCount" bson:"maxMediumCount"`
	DoneMediumCount int32 `json:"doneMediumCount" bson:"doneMediumCount"`
	MaxHardCount    int32 `json:"maxHardCount" bson:"maxHardCount"`
	DoneHardCount   int32 `json:"doneHardCount" bson:"doneHardCount"`
}

type GetUserRankResponse struct {
	Username    string `json:"username" bson:"username"`
	CountryRank int64  `json:"countryRank" bson:"countryRank"`
	GlobalRank  int64  `json:"globalRank" bson:"globalRank"`
}
