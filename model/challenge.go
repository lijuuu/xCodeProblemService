package model

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// challenge represents a coding challenge
type Challenge struct {
	ID                  primitive.ObjectID                    `bson:"_id,omitempty"`
	Title               string                                `bson:"title"`
	CreatorID           string                                `bson:"creator_id"`
	Difficulty          string                                `bson:"difficulty"`
	IsPrivate           bool                                  `bson:"is_private"`
	RoomCode            string                                `bson:"room_code"`
	ProblemIDs          []string                              `bson:"problem_ids"`
	TimeLimit           int32                                 `bson:"time_limit"`
	CreatedAt           int64                                 `bson:"created_at"`
	IsActive            bool                                  `bson:"is_active"`
	ParticipantIDs      []string                              `bson:"participant_ids"`
	UserProblemMetadata map[string][]ChallengeProblemMetadata `bson:"user_problem_metadata"`
	Status              string                                `bson:"status"`
	StartTime           int64                                 `bson:"start_time"`
	EndTime             int64                                 `bson:"end_time"`
	Password            string                                `bson:"password"`
	DeletedAt           *time.Time                            `bson:"deleted_at,omitempty"`
	UpdatedAt           int64                                 `bson:"updated_at"`
}

// problem metadata tracks user performance on a problem
type ChallengeProblemMetadata struct {
	ProblemID   string `bson:"problem_id"`
	Score       int    `bson:"score"`
	TimeTaken   int64  `bson:"time_taken"`
	CompletedAt int64  `bson:"completed_at"`
}

// user stats aggregates user performance across challenges
type UserStats struct {
	UserID              string                   `bson:"user_id"`
	ProblemsCompleted   int32                    `bson:"problems_completed"`
	TotalTimeTaken      int64                    `bson:"total_time_taken"`
	ChallengesCompleted int32                    `bson:"challenges_completed"`
	Score               float64                  `bson:"score"`
	ChallengeStats      map[string]ChallengeStat `bson:"challenge_stats"`
}

// challenge stat tracks user performance in a specific challenge
type ChallengeStat struct {
	Rank              int32 `bson:"rank"`
	ProblemsCompleted int32 `bson:"problems_completed"`
	TotalScore        int32 `bson:"total_score"`
}

// leaderboard entry for ranking users in a challenge
type LeaderboardEntry struct {
	UserID            string `bson:"user_id"`
	ProblemsCompleted int32  `bson:"problems_completed"`
	TotalScore        int32  `bson:"total_score"`
	Rank              int32  `bson:"rank"`
}
