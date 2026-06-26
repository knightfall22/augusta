package domain

import "time"

// Lease represents the current state of the lease
type Lease struct {
	//Represents current holder of the lease
	CandidateID string

	//Represents the last time the lease was renewed. Is nil when the lease is first created,
	// or is aquired by a new holder.
	LastRenewed time.Time

	//Represents the last time the lease was aquired
	LastAquired time.Time

	//Represents the expiration time of the lease
	ExpiresAt time.Time
}

// IsHolder returns true if the lease is currently held by the candidate
func (l *Lease) IsHolder(c string) bool {
	return l.CandidateID == c
}
