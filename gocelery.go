// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker  CeleryBroker
	backend CeleryBackend
	worker  *CeleryWorker
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	SendCeleryMessage(*CeleryMessage) error
	GetTaskMessage() (*TaskMessage, error) // must be non-blocking
	Listen(queues ...string) error
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(string) (*ResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *ResultMessage) error
}

type TaskResultError struct {
	Err interface{} `json:"err"`
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend, numWorkers int) (*CeleryClient, error) {
	return &CeleryClient{
		broker,
		backend,
		NewCeleryWorker(broker, backend, numWorkers),
	}, nil
}

// Register task
func (cc *CeleryClient) Register(name string, config *CeleryTaskConfig) {
	if config.Queue == "" {
		config.Queue = DefaultBrokerQueue
	}
	cc.worker.Register(name, config)
}

// StartWorkerWithContext starts celery workers with given parent context
func (cc *CeleryClient) StartWorkerWithContext(ctx context.Context) {
	cc.worker.StartWorkerWithContext(ctx)
}

// StartWorker starts celery workers
func (cc *CeleryClient) StartWorker() {
	cc.worker.StartWorker()
}

// StopWorker stops celery workers
func (cc *CeleryClient) StopWorker() {
	cc.worker.StopWorker()
}

// WaitForStopWorker waits for celery workers to terminate
func (cc *CeleryClient) WaitForStopWorker() {
	cc.worker.StopWait()
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, params *TaskParameters) (*AsyncResult, error) {
	if params == nil {
		params = &TaskParameters{}
	}
	celeryTask := getTaskMessage(task, params)
	return cc.delay(celeryTask)
}

func (cc *CeleryClient) delay(task *TaskMessage) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain. Any fields on
// instances of this struct will NOT be thread-safe.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {
	//ParseKwargs converts kwargs to an input to be passed to RunTask
	ParseKwargs(map[string]interface{}) (interface{}, error)
	//RunTask takes the output of ParsKwargs and returns the task's output
	RunTask(interface{}) (interface{}, error)
}

// AsyncResult represents pending result
type AsyncResult struct {
	TaskID  string
	backend CeleryBackend
	result  *ResultMessage
}

// Builds asynchronous result
func (cc *CeleryClient) GetAsyncResult(taskID string) *AsyncResult {
	return &AsyncResult{TaskID: taskID, backend: cc.backend}
}

// Get gets actual result from backend
// It blocks for period of time set by timeout and returns error if unavailable
func (ar *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			err := fmt.Errorf("%v timeout getting result for %s", timeout, ar.TaskID)
			return nil, err
		case <-ticker.C:
			val, err := ar.AsyncGet()
			if err != nil {
				if _, ok := err.(*TaskResultError); !ok {
					continue
				}
			}
			return val, err
		}
	}
}

// AsyncGet gets actual result from backend and returns nil if not available
func (ar *AsyncResult) AsyncGet() (interface{}, error) {
	if ar.result == nil {
		val, err := ar.backend.GetResult(ar.TaskID)
		if err != nil || val == nil {
			return val, err
		}
		if val.Status == "SUCCESS" || val.Status == "FAILURE" {
			ar.result = val
		}
	}
	if ar.result.Status != "SUCCESS" {
		return nil, &TaskResultError{Err: ar.result.Result}
	}
	return ar.result.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() (bool, error) {
	_, err := ar.AsyncGet()
	if ar.result != nil {
		return true, nil
	}
	return false, err
}

func (e *TaskResultError) Error() string {
	bytes, _ := json.Marshal(e.Err)
	return string(bytes)
}
