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
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	ProblemID   string             `bson:"problemId" json:"problemId"`
	Title       string             `bson:"title" json:"title"`
	Language    string             `bson:"language" json:"language"`
	Difficulty  string             `bson:"difficulty" json:"difficulty"`
	SubmittedAt time.Time          `bson:"submittedAt" json:"submittedAt"`
	UserID      string             `bson:"userId" json:"userId"`
	Score       int                `json:"score" bson:"score"`
}

type Submission struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID        string             `bson:"userId" json:"userId"`
	ProblemID     string             `bson:"problemId" json:"problemId"`
	ChallengeID   *string            `bson:"challengeid,omitempty" json:"challengeId,omitempty"`
	SubmittedAt   time.Time          `bson:"submittedAt" json:"submittedAt"`
	Status        string             `bson:"status" json:"status"`
	Score         int                `bson:"score" json:"score"`
	Language      string             `json:"language" bson:"language"`
	Output        string             `json:"output,omitempty" bson:"output"`
	ExecutionTime float64            `json:"executionTime,omitempty" bson:"execution_time"`
	Difficulty    string             `json:"difficulty" bson:"difficulty"`
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

type ExecutionResult struct {
	TotalTestCases  int  `json:"totalTestCases"`
	PassedTestCases int  `json:"passedTestCases"`
	FailedTestCases int  `json:"failedTestCases"`
	OverallPass     bool `json:"overallPass"`
}
