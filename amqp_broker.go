// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

//AMQPCeleryBroker is Broker client for AMQP
type AMQPCeleryBroker struct {
	connection     *amqp.Connection
	exchange       *AMQPExchange
	prefetchCount  int
	listener       chan interface{}
	listenerQueues []string
	connect        func() (bool, error)
}

type AMQPBrokerConfig struct {
	//URL url of amqp broker
	URL string
	//Exchange optional non-default exchange name
	Exchange         string
	ConnectionConfig *amqp.Config
}

// NewAMQPCeleryBroker creates new AMQPCeleryBroker using AMQP conn and channel
func NewAMQPCeleryBroker(config *AMQPBrokerConfig) (*AMQPCeleryBroker, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	broker := &AMQPCeleryBroker{
		exchange:      NewAMQPExchange(config.Exchange),
		listener:      make(chan interface{}),
		prefetchCount: 4,
	}
	connMutex := new(sync.Mutex)
	broker.connect = func() (bool, error) {
		connMutex.Lock()
		defer connMutex.Unlock()
		return broker.lazyConnect(config.URL, config.ConnectionConfig)
	}
	return broker, nil
}

// Listen spawns receiving channel on AMQP queue
func (b *AMQPCeleryBroker) Listen(queues ...string) error {
	b.listenerQueues = append(b.listenerQueues, queues...)
	if len(b.listenerQueues) == 0 {
		return nil
	}
	if reconnected, err := b.connect(); err != nil {
		return err
	} else if !reconnected {
		return b.registerConsumers(queues)
	}
	return nil
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	if _, err := b.connect(); err != nil {
		return err
	}
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
	if err = createQueue(b.exchange, NewAMQPQueue(taskMessage.Queue), channel); err != nil {
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
		b.exchange.Name,
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
func createQueue(exchange *AMQPExchange, queue *AMQPQueue, channel *amqp.Channel) error {
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
		exchange.Name,
		false,
		nil,
	)
	return err
}

func (b *AMQPCeleryBroker) openChannel() (*amqp.Channel, error) {
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
func (b *AMQPCeleryBroker) lazyConnect(host string, config *amqp.Config) (bool, error) {
	if b.connection == nil || b.connection.IsClosed() {
		logrus.Infof("Establishing rabbitmq connection to %s", host)
		connection, err := amqp.DialConfig(host, *config)
		if err != nil {
			return true, err
		}
		b.connection = connection
		// if we have listener queues, re-establish their connections
		if len(b.listenerQueues) > 0 {
			err = b.registerConsumers(b.listenerQueues)
		}
		return true, err
	}
	return false, nil
}

func (b *AMQPCeleryBroker) registerConsumers(queues []string) error {
	if len(queues) == 0 {
		return nil
	}
	channel, err := b.openChannel()
	if err != nil {
		return err
	}
	for _, queue := range queues {
		if err = createQueue(b.exchange, NewAMQPQueue(queue), channel); err != nil {
			return err
		}
		listener, err := channel.Consume(queue, generateConsumerTag(queue), false, false, false, true, nil)
		if err != nil {
			return err
		}
		go b.handleMessage(queue, listener)
		logrus.Debugf("started listener for queue %s", queue)
	}
	return nil
}

func (b *AMQPCeleryBroker) handleMessage(queue string, listener <-chan amqp.Delivery) {
	for delivery := range listener {
		deliveryAck(delivery)
		var taskMessage *TaskMessage
		err := json.Unmarshal(delivery.Body, &taskMessage)
		if err != nil {
			b.listener <- err
		}
		b.listener <- taskMessage
	}
	logrus.Infof("listener for queue %s lost connection", queue)
}

func generateConsumerTag(queue string) string {
	u, _ := uuid.NewV4()
	return "go-celery-consumer::" + queue + "::" + u.String()
}

func validateConfig(config *AMQPBrokerConfig) error {
	if config.URL == "" {
		return fmt.Errorf("empty amqp URL provided")
	}
	if config.Exchange == "" {
		config.Exchange = defaultExchange
	}
	if config.ConnectionConfig == nil {
		config.ConnectionConfig = &amqp.Config{}
	}
	return nil
}
