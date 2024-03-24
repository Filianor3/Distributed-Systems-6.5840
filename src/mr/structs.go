package mr

import "time"

type MapInfo struct {
	FileName     string
	FileIndex    int
	CurrentState int // 0 -pending 1 - working 2-finished
	StartTime    time.Time
}

type ReduceInfo struct {
	// FileName string
	// FileIndex int
	CurrentState int //
	ReduceTaskId int
	StartTime    time.Time
}
