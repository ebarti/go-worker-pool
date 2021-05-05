package workerpool

import (
	"github.com/vbauerster/mpb/v6"
	"io"
	"os"
	"reflect"
)

// NewInterceptor : workerPool factory. Needs a defined number of workers to instantiate.
func NewInterceptor() *Interceptor {
	return &Interceptor{}
}

// Interceptor is a dummy worker for testing
type Interceptor struct {
	out []interface{}
}

// Send : dummy implementation of a WorkerPool
func (i *Interceptor) Send(in interface{}) {
	_ = in
}

// ReceiveFrom : dummy implementation of a WorkerPool
func (i *Interceptor) ReceiveFrom(inWorker ...WorkerPool) WorkerPool {
	_ = inWorker
	return i
}

// ReceiveFromWithType : dummy implementation of a WorkerPool
func (i *Interceptor) ReceiveFromWithType(t reflect.Type, inWorker ...WorkerPool) WorkerPool {
	_ = t
	_ = inWorker
	return i
}

// Work : dummy implementation of a WorkerPool
func (i *Interceptor) Work() WorkerPool {
	return i
}

// OutChannel : dummy implementation of a WorkerPool
func (i *Interceptor) OutChannel(out chan interface{}) {
	_ = out
}

// OutChannelWithType : dummy implementation of a WorkerPool
func (i *Interceptor) OutChannelWithType(t reflect.Type, out chan interface{}) {
	_ = t
	_ = out
}

// Out : dummy implementation of a WorkerPool
func (i *Interceptor) Out(out interface{}) {
	i.out = append(i.out, out)
}

// CancelOnSignal : dummy implementation of a WorkerPool
func (i *Interceptor) CancelOnSignal(signals ...os.Signal) WorkerPool {
	_ = signals
	return i
}

// Wait : dummy implementation of a WorkerPool
func (i *Interceptor) Wait() (err error) {
	return nil
}

// IsDone : dummy implementation of a WorkerPool
func (i *Interceptor) IsDone() <-chan struct{} {
	return nil
}

// SetWriterOut : dummy implementation of a WorkerPool
func (i *Interceptor) SetWriterOut(writer io.Writer) WorkerPool {
	_ = writer
	return i
}

// Print : dummy implementation of a WorkerPool
func (i *Interceptor) Print(msg string) {
	_ = msg
}

// PrintByte : dummy implementation of a WorkerPool
func (i *Interceptor) PrintByte(msg []byte) {
	_ = msg
}

// Close : dummy implementation of a WorkerPool
func (i *Interceptor) Close() error {
	return nil
}

// IncrementExpectedTotalBy : dummy implementation of a WorkerPool
func (i *Interceptor) IncrementExpectedTotalBy(incrBy int) {
	// Noop
	_ = incrBy
}

// SetProgressDone : dummy implementation of a WorkerPool
func (i *Interceptor) SetProgressDone() {
	// Noop
}

// BuildBar : dummy implementation of a WorkerPool
func (i *Interceptor) BuildBar(total int, p *mpb.Progress, options ...mpb.BarOption) WorkerPool {
	_, _, _ = total, p, options
	// Noop
	return i
}

// Progress : dummy implementation of a WorkerPool
func (i *Interceptor) Progress() ProgressWorker {
	return i
}

// SetDone : dummy implementation of a WorkerPool
func (i *Interceptor) SetDone() {
	// Noop
}

// GetOut : use this in our tests to retrieve the outfeeded values
func (i *Interceptor) GetOut() []interface{} {
	return i.out
}
