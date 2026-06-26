package internal

import "errors"

var ErrCannotAquireLock = errors.New("cannot aquire lock")
var ErrNoLeaseFound = errors.New("no lease found")

var ErrNoTaskFound = errors.New("no task found")
