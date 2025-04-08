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

type ExecutionResultJSON struct {
	TestCaseIndex int    `json:"testCaseIndex"`
	Nums          []int  `json:"nums"`
	Target        int    `json:"target"`
	Expected      []int  `json:"expected"`
	Received      []int  `json:"received"`
	Passed        bool   `json:"passed"`
	Summary       string `json:"summary,omitempty"`
}
