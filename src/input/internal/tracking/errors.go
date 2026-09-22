package tracking

import "fmt"

type TrackingError struct {
	level       string // The level where the error occurred
	description string // What went wrong
}

func newTrackingError(location, description string) error {
	return &TrackingError{
		level:       location,
		description: description,
	}
}

func (e *TrackingError) Error() string {
	return fmt.Sprintf("A tracking error occurred at the %v level - %v", e.level, e.description)
}
