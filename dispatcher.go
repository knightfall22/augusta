package augusta

import (
	"context"
	"log"
	"time"

	"github.com/knightfall22/augusta/internal"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sirupsen/logrus"
)

type Dispatcher struct {
	Store internal.StorageEngine

	logger  *logrus.Entry
	timeout int
	done    chan struct{}
}

func NewDispatcher(store internal.StorageEngine, timeout int, logger *logrus.Entry) *Dispatcher {
	return &Dispatcher{
		Store:   store,
		timeout: timeout,
		done:    make(chan struct{}, 1),
		logger:  logger,
	}
}
func (p *Dispatcher) Run(ctx context.Context) {
	go p.run(ctx)
	go p.reaper(ctx)
}

func (p *Dispatcher) run(ctx context.Context) {
	logger := p.logger.WithContext(ctx).WithField("method", "run")
	for {
		select {
		case <-time.After(time.Duration(p.timeout) * time.Second):
			tasks, err := p.Store.GetPendingTasks(ctx)
			if err != nil {
				logger.Error(err)
				return
			}

			p.dispatch(ctx, tasks)

		case <-ctx.Done():
			logger.Info("Context Cancelled Stopping dispatcher")
			return
		case <-p.done:
			logger.Info("Dispatcher Stopped")
			return

		}
	}
}

func (p *Dispatcher) reaper(ctx context.Context) {
	logger := p.logger.WithContext(ctx).WithField("method", "reaper")
	reaperTimeout := time.Duration((p.timeout * 2)) * time.Second
	for {
		select {
		case <-time.After(reaperTimeout):
			if err := p.Store.GetLeaseExpiredTasks(ctx); err != nil {
				logger.Error(err)
				return
			}
		case <-ctx.Done():
			logger.Info("Context Cancelled Stopping reaper")
			return
		}
	}
}

func (p *Dispatcher) dispatch(ctx context.Context, tasks []*domain.Task) {
	log.Printf("[INFO] Dispatching task %+v\n", tasks)
}

func (p *Dispatcher) Stop() {
	p.done <- struct{}{}
}
