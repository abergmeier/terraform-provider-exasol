package db

import "errors"

var (
	ErrorNamedObjectNotFound = errors.New("Named object not found on database")
)
