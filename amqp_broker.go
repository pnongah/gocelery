// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const exchangeName = "celery"

// AMQPExchange stores AMQP Exchange configuration
type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

// NewAMQPExchange creates new AMQPExchange
func NewAMQPExchange(name string) *AMQPExchange {
	return &AMQPExchange{
		Name:       name,
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
	}
}

// AMQPQueue stores AMQP Queue configuration
type AMQPQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

// NewAMQPQueue creates new AMQPQueue
func NewAMQPQueue(name string) *AMQPQueue {
	return &AMQPQueue{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
	}
}

//AMQPCeleryBroker is Broker client for AMQP
type AMQPCeleryBroker struct {
	connection *amqp.Connection

	exchange *AMQPExchange
	//queue         *AMQPQueue
	prefetchCount int
	listener      chan interface{}
	connect       func() error
}

// NewAMQPCeleryBroker creates new AMQPCeleryBroker using AMQP conn and channel
func NewAMQPCeleryBroker(host string, config *amqp.Config) (*AMQPCeleryBroker, error) {
	broker := &AMQPCeleryBroker{
		exchange:      NewAMQPExchange(exchangeName),
		listener:      make(chan interface{}),
		prefetchCount: 4,
	}
	connMutex := new(sync.Mutex)
	broker.connect = func() error {
		connMutex.Lock()
		defer connMutex.Unlock()
		return broker.lazyConnect(host, config)
	}
	return broker, nil
}

// Listen spawns receiving channel on AMQP queue
func (b *AMQPCeleryBroker) Listen(queues []string) error {
	channel, err := b.openChannel()
	if err != nil {
		return err
	}
	for _, queue := range queues {
		if err = createQueue(NewAMQPQueue(queue), channel); err != nil {
			return err
		}
	}
	for _, queue := range queues {
		listener, err := channel.Consume(queue, generateConsumerTag(queue), false, false, false, true, nil)
		if err != nil {
			return err
		}
		go func() {
			for delivery := range listener {
				deliveryAck(delivery)
				var taskMessage *TaskMessage
				err := json.Unmarshal(delivery.Body, &taskMessage)
				if err != nil {
					b.listener <- err
				}
				b.listener <- taskMessage
			}
		}()
		logrus.Debugf("started listener for queue %s", queue)
	}
	if err != nil {
		return err
	}
	return nil
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	taskMessage := message.GetTaskMessage()
	channel, err := b.openChannel()
	if err != nil {
		return err
	}
	defer func() {
		err = channel.Close()
		if err != nil {
			logrus.Warnf("error closing channel")
		}
	}()
	if err = createQueue(NewAMQPQueue(taskMessage.Queue), channel); err != nil {
		return err
	}
	resBytes, err := json.Marshal(taskMessage)
	if err != nil {
		return err
	}
	publishMessage := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         resBytes,
	}
	return channel.Publish(
		exchangeName,
		taskMessage.Queue,
		false,
		false,
		publishMessage,
	)
}

// GetTaskMessage retrieves task message from AMQP queue
func (b *AMQPCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	select {
	case delivery := <-b.listener:
		if err, ok := delivery.(error); ok {
			return nil, err
		}
		return delivery.(*TaskMessage), nil
	default:
		return nil, fmt.Errorf("consuming channel is empty")
	}
}

// createExchange declares AMQP exchange with stored configuration
func createExchange(exchange *AMQPExchange, channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		exchange.Durable,
		exchange.AutoDelete,
		false,
		false,
		nil,
	)
}

// createQueue declares AMQP Queue with stored configuration
func createQueue(queue *AMQPQueue, channel *amqp.Channel) error {
	_, err := channel.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.AutoDelete,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	err = channel.QueueBind(
		queue.Name,
		queue.Name,
		exchangeName,
		false,
		nil,
	)
	return err
}

func (b *AMQPCeleryBroker) openChannel() (*amqp.Channel, error) {
	if err := b.connect(); err != nil {
		return nil, err
	}
	channel, err := b.connection.Channel()
	if err != nil {
		return nil, err
	}
	if err = createExchange(b.exchange, channel); err != nil {
		return nil, err
	}
	if err = channel.Qos(b.prefetchCount, 0, false); err != nil {
		return nil, err
	}
	return channel, nil
}

// lazyConnect connects to rabbitmq and handles recovery
func (b *AMQPCeleryBroker) lazyConnect(host string, config *amqp.Config) error {
	if b.connection == nil || b.connection.IsClosed() {
		connection, err := amqp.DialConfig(host, *config)
		if err != nil {
			return err
		}
		b.connection = connection
		//if len(b.listeners) > 0 {
		//	// implies that we were previously subscribed but lost connection, so restore it
		//	queues := make([]string, len(b.listeners))
		//	for k := range b.listeners {
		//		queues = append(queues, k)
		//	}
		//	if err = b.Listen(queues); err != nil {
		//		return err
		//	}
		//}
	}
	return nil
}

func generateConsumerTag(queue string) string {
	u, _ := uuid.NewV4()
	return "go-consumer::" + queue + "::" + u.String()
}
