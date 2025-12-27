package workflow

import "errors"

var (
	ErrNotFound      = errors.New("flow not found")
	ErrInvalidState  = errors.New("invalid flow state transition")
	ErrAlreadyExists = errors.New("flow already exists")
)
