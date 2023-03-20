package bus

import "errors"

const (
	NAME = "BUS"
)

var (
	//
	errInvalidConnection = errors.New("Invalid bus connection.")
)
