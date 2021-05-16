package workerpool

import (
	"context"
	"errors"
	"fmt"
	"github.com/vbauerster/mpb/v6"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
)

const (
	workerCount = 1000
	RunTimes    = 100000
)

type type1 string
type type2 string

type workerTest struct {
	name             string
	task             Task
	numWorkers       int64
	errExpected      bool
	typeConvFunction func(int) interface{}
}

type TestTaskObject struct {
	testTask func(in interface{}, out chan<- interface{}) error
}

type TestTaskObjectOutputSave struct {
	testTask func(in interface{}, out chan<- interface{}) error
	mu       *sync.RWMutex
	outs     []interface{}
}

var (
	t1                  type1
	t2                  type2
	testErr             = errors.New("test error")
	workerTestScenarios = []workerTest{
		{
			name:       "work basic",
			task:       NewTestTask(workBasic()),
			numWorkers: workerCount,
			typeConvFunction: func(i int) interface{} {
				return i
			},
		},
		{
			name:       "work basic type 1",
			task:       NewTestTask(workBasicType1()),
			numWorkers: workerCount,
			typeConvFunction: func(i int) interface{} {
				return type1(strconv.Itoa(i))
			},
		},
		{
			name:       "work basic type 2",
			task:       NewTestTask(workBasicType2()),
			numWorkers: workerCount,
			typeConvFunction: func(i int) interface{} {
				return type2(strconv.Itoa(i))
			},
		},
		{
			name:        "work with return of error",
			task:        NewTestTask(workWithError(testErr)),
			errExpected: true,
			numWorkers:  workerCount,
			typeConvFunction: func(i int) interface{} {
				return i
			},
		},
	}

	getWorker = func(ctx context.Context, wt workerTest) WorkerPool {
		worker := NewWorkerPool(ctx, wt.task, wt.numWorkers)
		return worker
	}
)

func NewTestTask(wf func(in interface{}, out chan<- interface{}) error) *TestTaskObject {
	return &TestTaskObject{wf}
}

func NewTestTaskObjectOutputSave(wf func(in interface{}, out chan<- interface{}) error) *TestTaskObjectOutputSave {
	return &TestTaskObjectOutputSave{testTask: wf, outs: []interface{}{}, mu: new(sync.RWMutex)}
}

func (tw *TestTaskObject) Run(in interface{}, out chan<- interface{}) error {
	return tw.testTask(in, out)
}

func (tw *TestTaskObjectOutputSave) Run(in interface{}, out chan<- interface{}) error {
	_ = out
	localOutChan := make(chan interface{}, 1)
	if err := tw.testTask(in, localOutChan); err != nil {
		return err
	}
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.outs = append(tw.outs, <-localOutChan)
	return nil
}

func workBasic() func(in interface{}, out chan<- interface{}) error {
	return func(in interface{}, out chan<- interface{}) error {
		out <- in
		return nil
	}
}

func workWithError(err error) func(in interface{}, out chan<- interface{}) error {
	return func(in interface{}, out chan<- interface{}) error {
		i := in.(int)
		total := i * rand.Intn(1000)
		if i == 100 {
			return err
		}
		out <- total
		return nil
	}
}

func workMultipleTypeOutput() func(in interface{}, out chan<- interface{}) error {
	return func(in interface{}, out chan<- interface{}) error {
		i := in.(int)
		outType1 := type1(strconv.Itoa(i) + " type1")
		outType2 := type2(strconv.Itoa(i) + " type2")
		out <- outType1
		out <- outType2
		return nil
	}
}

func workBasicType1() func(in interface{}, out chan<- interface{}) error {
	return func(in interface{}, out chan<- interface{}) error {
		if i, ok := in.(type1); ok {
			out <- i
		}
		return nil
	}
}

func workBasicType2() func(in interface{}, out chan<- interface{}) error {
	return func(in interface{}, out chan<- interface{}) error {
		if i, ok := in.(type2); ok {
			out <- i
		}
		return nil
	}
}

func TestMain(m *testing.M) {
	debug.SetGCPercent(500)
	runtime.GOMAXPROCS(2)
	code := m.Run()
	os.Exit(code)
}

func TestWorkerPool_WorkersNoType(t *testing.T) {
	for _, tt := range workerTestScenarios {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			workerOne := getWorker(ctx, tt).Work()
			// always need a consumer for the out tests so using basic here.
			taskTwo := NewTestTaskObjectOutputSave(workBasic())
			workerTwo := NewWorkerPool(ctx, taskTwo, workerCount).ReceiveFrom(nil, workerOne).Work()

			for i := 0; i < RunTimes; i++ {
				workerOne.Send(tt.typeConvFunction(i))
			}

			if err := workerOne.Close(); err != nil && !tt.errExpected {
				fmt.Println(err)
				t.Fail()
			}
			if err := workerTwo.Close(); err != nil && !tt.errExpected {
				fmt.Println(err)
				t.Fail()
			}

			if !tt.errExpected && len(taskTwo.outs) != RunTimes {
				t.Errorf("did not get expected count for test %s. Wanted %d but got %d", tt.name, RunTimes, len(taskTwo.outs))
			}
		})
	}
}

func TestWorkerPool_WorkersWithType(t *testing.T) {
	ctx := context.Background()

	type1task := NewTestTaskObjectOutputSave(workBasicType1())
	type2task := NewTestTaskObjectOutputSave(workBasicType2())

	workerOne := NewWorkerPool(ctx, NewTestTask(workMultipleTypeOutput()), workerCount).Work()

	workerType1 := NewWorkerPool(ctx, type1task, workerCount).ReceiveFrom(reflect.TypeOf(t1), workerOne).Work()
	workerType2 := NewWorkerPool(ctx, type2task, workerCount).ReceiveFrom(reflect.TypeOf(t2), workerOne).Work()

	for i := 0; i < RunTimes; i++ {
		workerOne.Send(i)
	}
	if err := workerOne.Close(); err != nil {
		t.Error(err)
	}
	if err := workerType1.Close(); err != nil {
		t.Error(err)
	}
	if err := workerType2.Close(); err != nil {
		t.Error(err)
	}

	if len(type1task.outs) != RunTimes {
		t.Errorf("did not get expected count for task 1. Wanted %d but got %d", RunTimes, len(type1task.outs))
	}
	if len(type2task.outs) != RunTimes {
		t.Errorf("did not get expected count for task 2 Wanted %d but got %d", RunTimes, len(type2task.outs))
	}

	for _, v := range type1task.outs {
		if _, ok := v.(type1); !ok {
			t.Errorf("Error - mismatch of type 1")
		}
	}
	for _, v := range type2task.outs {
		if _, ok := v.(type2); !ok {
			t.Errorf("Error - mismatch of type 2")
		}
	}
}

func TestWorkerPool_WorkersWithTypeAndNoType(t *testing.T) {
	ctx := context.Background()
	type1task := NewTestTaskObjectOutputSave(workBasicType1())
	type2task := NewTestTaskObjectOutputSave(workBasicType2())
	workerOne := NewWorkerPool(ctx, NewTestTask(workMultipleTypeOutput()), 100).Work()
	workerType1 := NewWorkerPool(ctx, type1task, 100).ReceiveFrom(reflect.TypeOf(t1), workerOne).Work()
	workerType2 := NewWorkerPool(ctx, type2task, 100).ReceiveFrom(nil, workerOne).Work()
	for i := 0; i < RunTimes; i++ {
		workerOne.Send(i)
	}
	if err := workerOne.Close(); err != nil {
		t.Error(err)
	}
	if err := workerType1.Close(); err != nil {
		t.Error(err)
	}
	if err := workerType2.Close(); err != nil {
		t.Error(err)
	}
	if len(type1task.outs) != RunTimes {
		t.Errorf("did not get expected count for task 1. Wanted %d but got %d", RunTimes, len(type1task.outs))
	}
	if len(type2task.outs) != RunTimes {
		t.Errorf("did not get expected count for task 2 Wanted %d but got %d", RunTimes, len(type2task.outs))
	}
	for _, v := range type1task.outs {
		if _, ok := v.(type1); !ok {
			t.Errorf("Error - mismatch of type 1")
		}
	}
	for _, v := range type2task.outs {
		if _, ok := v.(type2); !ok {
			t.Errorf("Error - mismatch of type 2")
		}
	}
}

func BenchmarkGoWorkers(b *testing.B) {
	ctx := context.Background()
	workBasicNoOut := func(in interface{}, out chan<- interface{}) error {
		_ = in.(int)
		return nil
	}
	worker := NewWorkerPool(ctx, NewTestTask(workBasicNoOut), workerCount).Work()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < RunTimes; j++ {
			worker.Send(j)
		}
	}
	b.StopTimer()
	_ = worker.Close()
}

func Test_workerPool_BuildBar(t *testing.T) {
	worker := &workerPool{}
	if nil != worker.bar {
		t.Errorf("Bar was built before we wanted one!")
	}
	p := mpb.New()
	worker.BuildBar(10, p)
	if nil == worker.bar {
		t.Errorf("Failed to build bar")
	}
}

func Test_workerPool_IncrementProgressBar(t *testing.T) {
	worker := &workerPool{mu: new(sync.RWMutex)}
	p := mpb.New()
	worker.BuildBar(10, p)
	if nil == worker.bar {
		t.Errorf("Failed to build bar")
	}
	worker.IncrementProgressBar(1)
	if worker.bar.Completed() {
		t.Errorf("Should not be complete!")
	}
	worker.IncrementProgressBar(9)
	if !worker.bar.Completed() {
		t.Errorf("Should be complete!")
	}
}

func Test_workerPool_UpdateExpectedTotal(t *testing.T) {
	worker := &workerPool{mu: new(sync.RWMutex)}
	p := mpb.New()
	worker.BuildBar(10, p)
	if nil == worker.bar {
		t.Errorf("Failed to build bar")
	}
	worker.IncrementProgressBar(1)
	if worker.bar.Completed() {
		t.Errorf("Should not be complete!")
	}
	if err := worker.UpdateExpectedTotal(-1); err != nil {
		t.Errorf("Test failed with error ", err)
	}
	worker.IncrementProgressBar(9)
	if worker.bar.Completed() {
		t.Errorf("Should not be complete!")
	}
}
