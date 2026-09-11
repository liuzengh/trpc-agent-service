package application

import (
	"errors"
	"reflect"
	"regexp"
)

var (
	ErrRuntimeStarted      = errors.New("delivery runtime already started")
	ErrRuntimeStopped      = errors.New("delivery runtime stopped")
	ErrRuntimeDrainTimeout = errors.New("delivery runtime drain incomplete")
)
var runtimeIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func absent(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	}
	return false
}

func validRuntimePage(ids []string, after, next string, exhausted bool, limit int) bool {
	if len(ids) > limit {
		return false
	}
	previous := after
	for _, id := range ids {
		if !runtimeIdentifier.MatchString(id) || id <= previous {
			return false
		}
		previous = id
	}
	if len(ids) == 0 {
		return exhausted && (next == "" || next == after)
	}
	return next == previous
}
