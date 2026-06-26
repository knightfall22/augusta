package augusta

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/knightfall22/augusta/internal"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sirupsen/logrus"
)

var defaultLeaseDuration = 60

type Watcher struct {
	State state
	Err   error
}

// Elector is a leader election service
// Elector use a distributed lock lease to elect a leader.
type Elector struct {

	// CandidatedID is the unique identifier for this specific instance in the cluster
	// (usually a UUID). It is written to the database lock document to prove ownership.
	CandidatedID string

	// LeaseStorage is the storage engine
	LeaseStorage internal.LeaseStorage

	// LeaseDuration is the absolute Time-To-Live (TTL) for the lock.
	// If the active leader fails to renew the lock within this window,
	// the LeaseStore considers the leader dead, allowing followers to take over.
	LeaseDuration time.Duration

	// leaseRenewalTimeout defines how often the active leader's heartbeat loop
	// attempts to renew the lease. It should be significantly shorter than
	// LeaseDuration to provide a safety buffer against network latency and clock skew.
	leaseRenewalTimeout time.Duration

	Logger *logrus.Entry
}

func NewElector(candidateID string, leaseStorage internal.LeaseStorage, leaseDuration int, log *logrus.Entry) *Elector {

	if leaseDuration == 0 {
		leaseDuration = defaultLeaseDuration
	}

	// leaseRenewalTimeout defines how often the leader attempts to renew its lock.
	// It is calculated as 50% of the lease duration plus a random jitter.
	// 1. The 50% baseline ensures the leader renews well before the expiration,
	//    creating a safety buffer against clock skew and temporary network latency.
	// 2. The random jitter prevents a "thundering herd" scenario by ensuring that
	//    if multiple instances boot simultaneously, their database queries are staggered.
	leaseRenewalTimeout := time.Duration(leaseDuration/2+rand.IntN(leaseDuration-(leaseDuration/2))) * time.Second

	return &Elector{
		CandidatedID:        candidateID,
		LeaseStorage:        leaseStorage,
		LeaseDuration:       time.Duration(leaseDuration) * time.Second,
		leaseRenewalTimeout: leaseRenewalTimeout,
		Logger:              log.WithField("elector", candidateID),
	}
}

func (e *Elector) Run(ctx context.Context) chan Watcher {
	watcher := make(chan Watcher)

	go func() {
		defer close(watcher)
		for {
			select {
			case <-ctx.Done():
				e.Logger.Info("Stopping elector")
				e.releaseLease(ctx, e.CandidatedID)
				return
			case <-time.After(e.leaseRenewalTimeout):
				sig, err := e.acquireLeaseOrRenew(ctx, e.CandidatedID)
				if sig == Leader {
					e.Logger.Infof("Canditate %s is %s", e.CandidatedID, sig)
				}
				watcher <- Watcher{State: sig, Err: err}
			}
		}
	}()

	return watcher
}

func (e *Elector) acquireLeaseOrRenew(ctx context.Context, candidateID string) (state, error) {

	if err := e.LeaseStorage.AquireLease(ctx, &domain.Lease{
		CandidateID: candidateID,
		ExpiresAt:   time.Now().UTC().Add(e.LeaseDuration),
		LastAquired: time.Now().UTC(),
	}); err == nil {
		return Leader, nil
	}
	return Follower, internal.ErrCannotAquireLock
}

func (e *Elector) releaseLease(ctx context.Context, candidateID string) error {
	return e.LeaseStorage.DeleteLease(ctx, candidateID)
}
