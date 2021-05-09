package workerpool

import (
	"context"
	"errors"
	"github.com/vbauerster/mpb/v6"
	"os"
	"os/signal"
	"reflect"
	"sync"
)

const (
	sigChanBufferSize = 1
)

type notAssigned struct{}

var notAssignedType = reflect.TypeOf(notAssigned{})

type WorkerPool interface {
	Send(interface{})
	ReceiveFrom(inWorker ...WorkerPool) WorkerPool
	ReceiveFromWithType(t reflect.Type, inWorker ...WorkerPool) WorkerPool
	Work() WorkerPool
	OutChannel(t reflect.Type, out chan interface{})
	CancelOnSignal(signals ...os.Signal) WorkerPool
	Wait() (err error)
	IsDone() <-chan struct{}
	Close() error
	BuildBar(total int, p *mpb.Progress, options ...mpb.BarOption) WorkerPool
	IncrementExpectedTotalBy(incrBy int) error
	IncrBy(incrBy int)
}

// Task : interface to be implemented by a desired type
// Its Run method is called by workerPool onceErr the workers start
// and there is data sent to the channel via its Send method
type Task interface {
	Run(in interface{}, out chan<- interface{}) error
}

// workerPool : a pool of workers that asynchronously execute a given task
type workerPool struct {
	Ctx              context.Context
	stopReqCtx       context.Context
	outChanMuxCtx    context.Context
	workerTask       Task
	err              error
	numberOfWorkers  int64
	inChan           chan interface{}
	internalOutChan  chan interface{}
	outTypedChan     map[reflect.Type]chan interface{}
	sigChan          chan os.Signal
	cancel           context.CancelFunc
	requestStop      context.CancelFunc
	cancelOutChanMux context.CancelFunc
	semaphore        chan struct{}
	wg               *sync.WaitGroup
	muxWg            *sync.WaitGroup
	onceErr          *sync.Once

	onceBar          *sync.Once
	mu               *sync.RWMutex
	bar              *mpb.Bar
	expectedTotalBar int64
}

// NewWorkerPool : workerPool factory. Needs a defined number of workers to instantiate.
func NewWorkerPool(ctx context.Context, workerFunction Task, numberOfWorkers int64) WorkerPool {
	c, cancel := context.WithCancel(ctx)
	stopReqCtx, requestStop := context.WithCancel(ctx)
	outChanMuxCtx, cancelOutChanMux := context.WithCancel(ctx)
	return &workerPool{
		numberOfWorkers:  numberOfWorkers,
		Ctx:              c,
		stopReqCtx:       stopReqCtx,
		outChanMuxCtx:    outChanMuxCtx,
		workerTask:       workerFunction,
		inChan:           make(chan interface{}, numberOfWorkers),
		internalOutChan:  make(chan interface{}, numberOfWorkers),
		sigChan:          make(chan os.Signal, sigChanBufferSize),
		outTypedChan:     make(map[reflect.Type]chan interface{}),
		cancel:           cancel,
		requestStop:      requestStop,
		cancelOutChanMux: cancelOutChanMux,
		semaphore:        make(chan struct{}, numberOfWorkers),
		wg:               new(sync.WaitGroup),
		muxWg:            new(sync.WaitGroup),
		onceErr:          new(sync.Once),
	}
}

// Send : send to workers in channel
func (wp *workerPool) Send(in interface{}) {
	select {
	case <-wp.IsDone():
		return
	case wp.inChan <- in:
		return
	}
}

// ReceiveFrom : assigns workers out channel to this workers in channel
func (wp *workerPool) ReceiveFrom(inWorker ...WorkerPool) WorkerPool {
	for _, worker := range inWorker {
		worker.OutChannel(notAssignedType, wp.inChan)
	}
	return wp
}

// ReceiveFromWithType : assigns workers out channel to this workers in channel
func (wp *workerPool) ReceiveFromWithType(t reflect.Type, inWorker ...WorkerPool) WorkerPool {
	for _, worker := range inWorker {
		worker.OutChannel(t, wp.inChan)
	}
	return wp
}

// OutChannel : Sets the workers output channel to one provided.
// This can only be called onceErr per pool or an error will be returned.
func (wp *workerPool) OutChannel(t reflect.Type, out chan interface{}) {
	if _, ok := wp.outTypedChan[t]; !ok {
		wp.outTypedChan[t] = out
	}
}

// Work : starts workers
func (wp *workerPool) Work() WorkerPool {
	wp.runOutChanMux()
	go func() {
		wp.wg.Add(1)
		defer wp.wg.Done()
		defer wp.setDone()
		var wg = new(sync.WaitGroup)
		for {
			select {
			case <-wp.StopRequested():
				if len(wp.inChan) > 0 {
					continue
				}
				wg.Wait()
				wp.cancelOutChanMux()
				return
			case <-wp.IsDone():
				if wp.err == nil {
					wp.err = context.Canceled
				}
				return
			case in := <-wp.inChan:
				wp.semaphore <- struct{}{}
				wg.Add(1)
				go func(in interface{}) {
					defer func() {
						<-wp.semaphore
						wg.Done()
					}()
					if err := wp.workerTask.Run(in, wp.internalOutChan); err != nil {
						wp.onceErr.Do(func() {
							wp.err = err
							wp.cancel()
						})
						return
					}
				}(in)
			}
		}
	}()
	return wp
}

// out : pushes value to workers out channel
// Used when chaining worker pools.
func (wp *workerPool) out(out interface{}) error {
	selectedChan := wp.outTypedChan[notAssignedType]
	for k, t := range wp.outTypedChan {
		if k == reflect.TypeOf(out) {
			selectedChan = t
			break
		}
	}
	if selectedChan == nil {
		return errors.New("couldn't locate out chan")
	}
	select {
	case <-wp.IsDone():
		return nil
	case selectedChan <- out:
		return nil
	}
}

// waitForSignal : make sure we wait for a term signal and shutdown correctly
func (wp *workerPool) waitForSignal(signals ...os.Signal) {
	go func() {
		signal.Notify(wp.sigChan, signals...)
		<-wp.sigChan
		if wp.cancel != nil {
			wp.cancel()
		}
	}()
}

// CancelOnSignal : set the signal to be used to cancel the pool
func (wp *workerPool) CancelOnSignal(signals ...os.Signal) WorkerPool {
	if len(signals) > 0 {
		wp.waitForSignal(signals...)
	}
	return wp
}

// Wait : waits for all the workers to finish up
func (wp *workerPool) Wait() (err error) {
	wp.wg.Wait()
	wp.muxWg.Wait()
	return wp.err
}

// IsDone : returns a context's cancellation or error
func (wp *workerPool) IsDone() <-chan struct{} {
	return wp.Ctx.Done()
}

// StopRequested : returns a request to stop context's cancellation or error
func (wp *workerPool) StopRequested() <-chan struct{} {
	return wp.stopReqCtx.Done()
}

// IsOutMuxChanDone : returns a request to stop context's cancellation or error
func (wp *workerPool) IsOutMuxChanDone() <-chan struct{} {
	return wp.outChanMuxCtx.Done()
}

// Close : a channel if the receiver is looking for a close.
// It indicates that no more data follows.
func (wp *workerPool) Close() error {
	wp.requestStop()

	if err := wp.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (wp *workerPool) runOutChanMux() {
	go func() {
		wp.muxWg.Add(1)
		defer wp.muxWg.Done()
		for {
			select {
			case out := <-wp.internalOutChan:
				if err := wp.out(out); err != nil {
					wp.err = err
					wp.cancel()
				}
			case <-wp.IsOutMuxChanDone():
				if len(wp.internalOutChan) > 0 {
					continue
				}
				return
			case <-wp.IsDone(): // should never happen
				return
			}
		}
	}()
}

// BuildBar : creates a progress bar
func (wp *workerPool) BuildBar(total int, p *mpb.Progress, options ...mpb.BarOption) WorkerPool {
	wp.expectedTotalBar = int64(total)
	wp.onceBar = new(sync.Once)
	wp.bar = p.AddBar(int64(total), options...)
	return wp
}

// IncrementExpectedTotalBy : sets the total expected by the bar
func (wp *workerPool) IncrementExpectedTotalBy(incrBy int) error {
	if nil == wp.bar {
		return errors.New("no progress bar present")
	}
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.expectedTotalBar += int64(incrBy)
	wp.bar.SetTotal(wp.expectedTotalBar, false)
	return nil
}

// setDone : sets the progress nar as done
func (wp *workerPool) setDone() {
	if nil == wp.bar {
		return
	}
	wp.onceBar.Do(func() {
		wp.bar.SetTotal(0, true)
	})
}

// Increment : increments the progress bar current count by 1 (if existent)
func (wp *workerPool) Increment() {
	if nil == wp.bar {
		return
	}
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.bar.Increment()
}

// IncrBy : increments the progress bar current count by a number (if existent)
func (wp *workerPool) IncrBy(incrBy int) {
	if nil == wp.bar {
		return
	}
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.bar.IncrBy(incrBy)
}
