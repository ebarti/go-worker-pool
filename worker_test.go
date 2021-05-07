package workerpool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"testing"
)

const (
	workerCount = 1000
	RunTimes    = 100000
)

var (
	testErr             = errors.New("test error")
	workerTestScenarios = []workerTest{
		{
			name:       "work basic",
			task:       NewTestTask(workBasic()),
			numWorkers: workerCount,
		},
		{
			name:       "work basic with timeout",
			task:       NewTestTask(workBasic()),
			numWorkers: workerCount,
		},
		{
			name:        "work with return of error",
			task:        NewTestTask(workWithError(testErr)),
			errExpected: true,
			numWorkers:  workerCount,
		},
		{
			name:        "work with return of error with timeout",
			task:        NewTestTask(workWithError(testErr)),
			errExpected: true,
			numWorkers:  workerCount,
		},
	}

	getWorker = func(ctx context.Context, wt workerTest) WorkerPool {
		worker := NewWorkerPool(ctx, wt.task, wt.numWorkers)
		return worker
	}
)

type workerTest struct {
	name        string
	task        Task
	numWorkers  int64
	testSignal  bool
	errExpected bool
}

type TestTaskObject struct {
	testTask func(w WorkerPool, in interface{}) error
}

func NewTestTask(wf func(w WorkerPool, in interface{}) error) *TestTaskObject {
	return &TestTaskObject{wf}
}

func (tw *TestTaskObject) Run(w WorkerPool, in interface{}) error {
	return tw.testTask(w, in)
}

func workBasicNoOut() func(w WorkerPool, in interface{}) error {
	return func(w WorkerPool, in interface{}) error {
		_ = in.(int)
		return nil
	}
}

func workBasic() func(w WorkerPool, in interface{}) error {
	return func(w WorkerPool, in interface{}) error {
		i := in.(int)
		w.Out(i)
		return nil
	}
}

type type1 string
type type2 string

type TestTypeTaskObject struct {
	testTask func(w WorkerPool, in interface{}) error
}

func NewTestTypeTaskObject(wf func(w WorkerPool, in interface{}, out func(interface{})) error) *TestTypeTaskObject {
	return &TestTypeTaskObject{wf}
}

func (tw *TestTypeTaskObject) Run(w WorkerPool, in interface{}) error {
	return tw.testTask(w, in)
}

func workMultipleTypeOutput() func(w WorkerPool, in interface{}) error {
	return func(w WorkerPool, in interface{}) error {
		i := in.(int)
		outType1 := type1(strconv.Itoa(i) + " type1")
		outType2 := type2(strconv.Itoa(i) + " type2")
		w.Out(outType1)
		w.Out(outType2)
		return nil
	}
}

func workBasicType1() func(w WorkerPool, in interface{}, out func(interface{})) error {
	return func(w WorkerPool, in interface{}, out func(interface{})) error {
		i, ok := in.(type1)
		if !ok {
			return errors.New("Mismatch at Type1")
		}
		w.Out(i)
		return nil
	}
}

func workBasicType2() func(w WorkerPool, in interface{}, out func(interface{})) error {
	return func(w WorkerPool, in interface{}, out func(interface{})) error {
		i, ok := in.(type2)
		if !ok {
			return errors.New("Mismatch at Type2")
		}
		out(i)
		return nil
	}
}

func workWithError(err error) func(w WorkerPool, in interface{}) error {
	return func(w WorkerPool, in interface{}) error {
		i := in.(int)
		total := i * rand.Intn(1000)
		if i == 100 {
			return err
		}
		w.Out(total)
		return nil
	}
}

func TestMain(m *testing.M) {
	debug.SetGCPercent(500)
	runtime.GOMAXPROCS(2)
	code := m.Run()
	os.Exit(code)
}

func TestWorkers(t *testing.T) {
	for _, tt := range workerTestScenarios {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			workerOne := getWorker(ctx, tt).Work()
			// always need a consumer for the out tests so using basic here.
			workerTwo := NewWorkerPool(ctx, NewTestTask(workBasicNoOut()), workerCount).ReceiveFrom(workerOne).Work()

			for i := 0; i < RunTimes; i++ {
				workerOne.Send(i)
			}

			if err := workerOne.Close(); err != nil && !tt.errExpected {
				fmt.Println(err)
				t.Fail()
			}
			if err := workerTwo.Close(); err != nil && !tt.errExpected {
				fmt.Println(err)
				t.Fail()
			}
		})
	}
}

func TestWorkersWithType(t *testing.T) {
	ctx := context.Background()
	var t1 type1
	var t2 type2
	interceptor1 := NewInterceptor()
	interceptor2 := NewInterceptor()
	workerOne := NewWorkerPool(ctx, NewTestTask(workMultipleTypeOutput()), 100).Work()
	workerType1 := NewWorkerPool(ctx, NewTestTypeTaskObject(workBasicType1(), interceptor1), 100).ReceiveFromWithType(reflect.TypeOf(t1), workerOne).Work()
	workerType2 := NewWorkerPool(ctx, NewTestTypeTaskObject(workBasicType2(), interceptor2), 100).ReceiveFromWithType(reflect.TypeOf(t2), workerOne).Work()
	for i := 0; i < 2000; i++ {
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
	for _, v := range interceptor1.GetOut() {
		if _, ok := v.(type1); !ok {
			t.Errorf("Error - mismatch of type 1")
		}
	}
	for _, v := range interceptor2.GetOut() {
		if _, ok := v.(type2); !ok {
			t.Errorf("Error - mismatch of type 1")
		}
	}
}

func BenchmarkGoWorkers(b *testing.B) {
	ctx := context.Background()
	worker := NewWorkerPool(ctx, NewTestTask(workBasicNoOut()), workerCount).Work()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < RunTimes; j++ {
			worker.Send(j)
		}
	}
	b.StopTimer()
	_ = worker.Close()
}
