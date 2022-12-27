package common

import (
	"errors"
	"runtime"
	"strings"
)

var (
	ErrInternal = errors.New("internal server error")
	ErrNotFound = errors.New("not found")
)

// Frame is short format of runtime.Frime
type Frame struct {
	Function string
	File     string
	Line     int
}

// GetFrames function for retrieve calling trace,
// can be used if you want to write calling trace to log
func GetFrames() []Frame {
	const projectName = "myProject" // should be according to your project name

	maxLength := make([]uintptr, 99)
	// skip firs 2 callers which is "runtime.Callers" and common.GetFrames
	n := runtime.Callers(2, maxLength)

	var res []Frame
	if n > 0 {
		frames := runtime.CallersFrames(maxLength[:n])
		for more, frameIndex := true, 0; more; frameIndex++ {

			var frameCandidate runtime.Frame
			frameCandidate, more = frames.Next()

			// skip tracing when called function not from our project (as example external dependency gin, urfaveCli)
			if !strings.Contains(frameCandidate.Function, projectName) {
				break
			}
			res = append(res, Frame{
				Function: frameCandidate.Function,
				File:     frameCandidate.File,
				Line:     frameCandidate.Line,
			})
		}
	}

	return res
}
