// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	*redis.Pool
	defaultSendQueue string
	listenerQueues   []string
}

type RedisBrokerConfig struct {
	DefaultSendQueue string
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection pool
func NewRedisBroker(conn *redis.Pool, config *RedisBrokerConfig) *RedisCeleryBroker {
	if config.DefaultSendQueue == "" {
		config.DefaultSendQueue = DefaultBrokerQueue
	}
	return &RedisCeleryBroker{
		defaultSendQueue: DefaultBrokerQueue,
		Pool:             conn,
	}
}

func (cb *RedisCeleryBroker) Listen(queues ...string) error {
	cb.listenerQueues = append(cb.listenerQueues, queues...)
	return nil
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()
	taskMessage := message.GetTaskMessage()
	queue := cb.defaultSendQueue
	if taskMessage.Queue != "" {
		queue = taskMessage.Queue
	}
	_, err = conn.Do("LPUSH", queue, jsonBytes)
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	conn := cb.Get()
	defer conn.Close()
	var message CeleryMessage
	var errorMessages string
	for _, queue := range cb.listenerQueues {
		messageJSON, err := conn.Do("BRPOP", queue, "1")
		if err != nil {
			errorMessages += fmt.Sprintf("error dequeing queue %s: %v;", queue, err)
			// continue looping - perhaps other queues are ok
		} else if messageJSON == nil {
			// nothing in this queue - skip
			continue
		} else {
			messageList := messageJSON.([]interface{})
			if string(messageList[0].([]byte)) != queue {
				return nil, fmt.Errorf("not a celery message: %v", messageList[0])
			}
			if err = json.Unmarshal(messageList[1].([]byte), &message); err != nil {
				return nil, err
			}
			return &message, nil
		}
	}
	if errorMessages != "" {
		return nil, fmt.Errorf("error(s) encountered dequeuing the redis broker: %s", errorMessages)
	}
	return nil, fmt.Errorf("all queues are empty")
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage()
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}

// NewRedisPool creates pool of redis connections from given connection string
//
// Deprecated: newRedisPool exists for historical compatibility
// and should not be used. Pool should be initialized outside of gocelery package.
func NewRedisPool(uri string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(uri)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
