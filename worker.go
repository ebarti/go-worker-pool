package workerpool

import (
	"context"
	"errors"
	"github.com/ebarti/utils"
	"github.com/vbauerster/mpb/v6"
	"os"
	"os/signal"
	"reflect"
	"sync"
)

const (
	sigChanBufferSize = 1
)

type WorkerPool interface {
	Send(interface{})
	ReceiveFrom(t reflect.Type, inWorker ...WorkerPool) WorkerPool
	Work() WorkerPool
	OutChannel(t reflect.Type, out chan interface{})
	CancelOnSignal(signals ...os.Signal) WorkerPool
	Close() error
	BuildBar(total int, p *mpb.Progress, options ...mpb.BarOption) WorkerPool
	UpdateExpectedTotal(incrementTotalBy int) error
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
	workerTask       Task
	err              error
	numberOfWorkers  int64
	inChan           chan interface{}
	internalOutChan  chan interface{}
	outTypedChan     map[reflect.Type][]chan interface{}
	sigChan          chan os.Signal
	cancel           context.CancelFunc
	semaphore        chan struct{}
	isLeader         bool
	wg               *sync.WaitGroup
	onceErr          *sync.Once
	onceCloseOut     *sync.Once
	onceBar          *sync.Once
	mu               *sync.RWMutex
	bar              *mpb.Bar
	expectedTotalBar int64
}

// NewWorkerPool : workerPool factory. Needs a defined number of workers to instantiate.
func NewWorkerPool(ctx context.Context, workerFunction Task, numberOfWorkers int64) WorkerPool {
	c, cancel := context.WithCancel(ctx)
	return &workerPool{
		numberOfWorkers: numberOfWorkers,
		Ctx:             c,
		workerTask:      workerFunction,
		inChan:          make(chan interface{}, numberOfWorkers),
		internalOutChan: make(chan interface{}, numberOfWorkers),
		sigChan:         make(chan os.Signal, sigChanBufferSize),
		outTypedChan:    make(map[reflect.Type][]chan interface{}),
		cancel:          cancel,
		semaphore:       make(chan struct{}, numberOfWorkers),
		isLeader:        true,
		wg:              new(sync.WaitGroup),
		onceErr:         new(sync.Once),
		onceCloseOut:    new(sync.Once),
	}
}

// Send : send to workers in channel
func (wp *workerPool) Send(in interface{}) {
	select {
	case <-wp.Ctx.Done():
		return
	case wp.inChan <- in:
		return
	}
}

// ReceiveFrom : assigns workers out channel to this workers in channel
func (wp *workerPool) ReceiveFrom(t reflect.Type, inWorker ...WorkerPool) WorkerPool {
	wp.isLeader = false
	for _, worker := range inWorker {
		worker.OutChannel(t, wp.inChan)
	}
	return wp
}

// OutChannel : Sets the workers output channel to one provided.
// This can only be called onceErr per pool or an error will be returned.
func (wp *workerPool) OutChannel(t reflect.Type, out chan interface{}) {
	if _, ok := wp.outTypedChan[t]; !ok {
		var outs []chan interface{}
		outs = append(outs, out)
		wp.outTypedChan[t] = outs
	} else {
		wp.outTypedChan[t] = append(wp.outTypedChan[t], out)
	}
}

// Work : starts workers
func (wp *workerPool) Work() WorkerPool {
	wp.wg.Add(1)
	wp.runOutChanMux()
	go func() {
		var wg = new(sync.WaitGroup)
		defer func() {
			wg.Wait()
			wp.wg.Done()
			wp.notifyProgressBarDone()
		}()
		for in := range wp.inChan {
			select {
			case <-wp.Ctx.Done():
				wp.err = context.Canceled
				continue
			default:
				wp.semaphore <- struct{}{}
				wg.Add(1)
				go func(in interface{}) {
					defer func() {
						wp.incrementProgressBar(1)
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
	selectedChans := wp.outTypedChan[reflect.TypeOf(nil)]
	for k, t := range wp.outTypedChan {
		if k == reflect.TypeOf(out) {
			selectedChans = t
			break
		}
	}
	if selectedChans == nil || len(selectedChans) == 0 {
		return errors.New("couldn't locate out chan")
	}
	select {
	case <-wp.Ctx.Done():
		return nil
	default:
		utils.TeeValue(out, selectedChans...)
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

// closeChannels : waits for all the workers to finish up
func (wp *workerPool) closeChannels() (err error) {
	wp.onceCloseOut.Do(func() {
		close(wp.inChan)
		close(wp.internalOutChan)
	})
	wp.wg.Wait()
	return wp.err
}

// Close : a channel if the receiver is looking for a close.
// It indicates that no more data follows.
func (wp *workerPool) Close() error {
	for len(wp.inChan) > 0 || len(wp.semaphore) > 0 {
		// block
	}
	if err := wp.closeChannels(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// runOutChanMux : multiplex the out channel to the right destination
func (wp *workerPool) runOutChanMux() {
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		for out := range wp.internalOutChan {
			if err := wp.out(out); err != nil {
				wp.err = err
				wp.cancel()
			}
		}
	}()
}

// BuildBar : creates a progress bar
func (wp *workerPool) BuildBar(total int, p *mpb.Progress, options ...mpb.BarOption) WorkerPool {
	wp.expectedTotalBar = int64(total)
	wp.onceBar = new(sync.Once)
	wp.mu = new(sync.RWMutex)
	wp.bar = p.AddBar(int64(total), options...)
	return wp
}

// UpdateExpectedTotal : sets the total expected by the bar
func (wp *workerPool) UpdateExpectedTotal(incrementTotalBy int) error {
	if nil == wp.bar {
		return errors.New("no progress bar present")
	}
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.expectedTotalBar += int64(incrementTotalBy)
	wp.bar.SetTotal(wp.expectedTotalBar, false)
	return nil
}

// notifyProgressBarDone : sets the progress bar as done
func (wp *workerPool) notifyProgressBarDone() {
	if nil == wp.bar {
		return
	}
	wp.onceBar.Do(func() {
		wp.bar.SetTotal(wp.expectedTotalBar, true)
	})
}

// incrementProgressBar : increments the progress bar current count by a number (if existent)
func (wp *workerPool) incrementProgressBar(incrBy int) {
	if nil == wp.bar {
		return
	}
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.bar.IncrBy(incrBy)
}
