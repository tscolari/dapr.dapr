package actors

import (
	"context"
	"errors"
	"sync"
)

type bufferedWFRequest struct {
	Req        []byte
	WorkflowID string
}

type burstBuffer struct {
	buffer chan bufferedWFRequest
	ctx    context.Context
	stop   func()
}

func NewBurstBuffer(size int) *burstBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	return &burstBuffer{
		buffer: make(chan bufferedWFRequest, size),
		ctx:    ctx,
		stop:   cancel,
	}
}

func (b *burstBuffer) Add(id string, req []byte) error {
	select {
	case <-b.ctx.Done():
		return errors.New("burst buffer stopped")
	default:
		b.buffer <- bufferedWFRequest{WorkflowID: id, Req: req}
	}
	return nil
}

func (b *burstBuffer) Stop() {
	b.stop()
	close(b.buffer)
}

func (b *burstBuffer) Workers(numWorkers int, work func(ctx context.Context, workflowID string, req []byte) error) chan error {
	errors := make(chan error)

	wg := sync.WaitGroup{}
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				req, ok := <-b.buffer
				if !ok {
					return
				}

				err := work(b.ctx, req.WorkflowID, req.Req)
				if err != nil {
					errors <- err
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errors)
	}()

	return errors
}
