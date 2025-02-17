// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// CeleryWorker represents distributed task worker
type CeleryWorker struct {
	broker          CeleryBroker
	backend         CeleryBackend
	numWorkers      int
	registeredTasks map[string]*CeleryTaskConfig
	taskLock        sync.RWMutex
	cancel          context.CancelFunc
	workWG          sync.WaitGroup
	rateLimitPeriod time.Duration
}

type CeleryTaskConfig struct {
	Task  interface{}
	Queue string
}

// NewCeleryWorker returns new celery worker
func NewCeleryWorker(broker CeleryBroker, backend CeleryBackend, numWorkers int) *CeleryWorker {
	return &CeleryWorker{
		broker:          broker,
		backend:         backend,
		numWorkers:      numWorkers,
		registeredTasks: map[string]*CeleryTaskConfig{},
		rateLimitPeriod: 100 * time.Millisecond,
	}
}

// StartWorkerWithContext starts celery worker(s) with given parent context
func (w *CeleryWorker) StartWorkerWithContext(ctx context.Context) {
	if err := w.broker.Listen(w.getRegisteredQueues()...); err != nil {
		panic(err)
	}
	var wctx context.Context
	wctx, w.cancel = context.WithCancel(ctx)
	w.workWG.Add(w.numWorkers)
	for i := 0; i < w.numWorkers; i++ {
		go func(workerID int) {
			defer w.workWG.Done()
			ticker := time.NewTicker(w.rateLimitPeriod)
			for {
				select {
				case <-wctx.Done():
					return
				case <-ticker.C:
					// process task request
					taskMessage, err := w.broker.GetTaskMessage()
					if err != nil || taskMessage == nil {
						continue
					}
					// run task
					logrus.Debugf("Running task with ID %s", taskMessage.ID)
					resultMsg, err := w.RunTask(taskMessage)
					if err != nil {
						log.Printf("failed to run task message %s: %+v", taskMessage.ID, err)
						resultMsg = getResultMessage(&TaskResultError{Err: err.Error()})
						resultMsg.Status = "FAILURE"
					}
					defer releaseResultMessage(resultMsg)
					logrus.Debugf("Storing result for task ID %s into backend", taskMessage.ID)
					// push result to backend
					err = w.backend.SetResult(taskMessage.ID, resultMsg)
					if err != nil {
						log.Printf("failed to push result: %+v", err)
						continue
					}
				}
			}
		}(i)
	}
}

// StartWorker starts celery workers
func (w *CeleryWorker) StartWorker() {
	w.StartWorkerWithContext(context.Background())
}

// StopWorker stops celery workers
func (w *CeleryWorker) StopWorker() {
	w.cancel()
	w.workWG.Wait()
}

// StopWait waits for celery workers to terminate
func (w *CeleryWorker) StopWait() {
	w.workWG.Wait()
}

// GetNumWorkers returns number of currently running workers
func (w *CeleryWorker) GetNumWorkers() int {
	return w.numWorkers
}

// Register registers tasks (functions)
func (w *CeleryWorker) Register(name string, taskConfig *CeleryTaskConfig) {
	w.taskLock.Lock()
	w.registeredTasks[name] = taskConfig
	w.taskLock.Unlock()
}

// GetTask retrieves registered task
func (w *CeleryWorker) GetTask(name string) *CeleryTaskConfig {
	w.taskLock.RLock()
	task, ok := w.registeredTasks[name]
	if !ok {
		w.taskLock.RUnlock()
		return nil
	}
	w.taskLock.RUnlock()
	return task
}

// RunTask runs celery task
func (w *CeleryWorker) RunTask(message *TaskMessage) (*ResultMessage, error) {

	// ignore if the message is expired
	if message.Expires != nil && message.Expires.UTC().Before(time.Now().UTC()) {
		return nil, fmt.Errorf("task %s is expired on %s", message.ID, message.Expires)
	}

	// check for malformed task message - args cannot be nil
	if message.Args == nil {
		return nil, fmt.Errorf("task %s is malformed - args cannot be nil", message.ID)
	}

	// get task
	config := w.GetTask(message.Task)
	if config == nil {
		return nil, fmt.Errorf("task %s is not registered", message.Task)
	}

	// convert to task interface
	taskInterface, ok := config.Task.(CeleryTask)
	if ok {
		input, err := taskInterface.ParseKwargs(message.Kwargs)
		if err != nil {
			return nil, err
		}
		output, err := taskInterface.RunTask(input)
		if err != nil {
			return nil, err
		}
		return getResultMessage(output), nil
	}

	// use reflection to execute function ptr
	taskFunc := reflect.ValueOf(config.Task)
	return runTaskFunc(&taskFunc, message)
}

func runTaskFunc(taskFunc *reflect.Value, message *TaskMessage) (*ResultMessage, error) {

	// check number of arguments
	numArgs := taskFunc.Type().NumIn()
	messageNumArgs := len(message.Args)
	if numArgs != messageNumArgs {
		return nil, fmt.Errorf("Number of task arguments %d does not match number of message arguments %d",
			numArgs, messageNumArgs)
	}

	// construct arguments
	in := make([]reflect.Value, messageNumArgs)
	for i, arg := range message.Args {
		origType := taskFunc.Type().In(i).Kind()
		msgType := reflect.TypeOf(arg).Kind()
		// special case - convert float64 to int if applicable
		// this is due to json limitation where all numbers are converted to float64
		if origType == reflect.Int && msgType == reflect.Float64 {
			arg = int(arg.(float64))
		}
		if origType == reflect.Float32 && msgType == reflect.Float64 {
			arg = float32(arg.(float64))
		}

		in[i] = reflect.ValueOf(arg)
	}

	// call method
	res := taskFunc.Call(in)
	if len(res) == 0 {
		return nil, nil
	}
	errorReturn := res[len(res)-1].Type().Implements(reflect.TypeOf((*error)(nil)).Elem())
	if len(res) > 2 {
		return nil, fmt.Errorf("only [Any] or [Any, error] return signatures are supported")
	}
	if errorReturn {
		if err := res[len(res)-1].Interface(); err != nil {
			return nil, err.(error)
		}
	}
	return getReflectionResultMessage(&res[0]), nil
}

func (w *CeleryWorker) getRegisteredQueues() []string {
	queuesMap := map[string]interface{}{}
	for _, v := range w.registeredTasks {
		queuesMap[v.Queue] = nil
	}
	var queues []string
	for k := range queuesMap {
		queues = append(queues, k)
	}
	return queues
}
