package config

import (
	"fmt"
	"github.com/pnongah/gocelery"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/keon94/go-compose/docker"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var Env = &docker.EnvironmentConfig{
	UpTimeout:   60 * time.Second,
	DownTimeout: 60 * time.Second,
	ComposeFilePaths: []string{
		"docker-compose.yml",
	},
}

func GetRabbitMQConnectionConfig(c *docker.Container) (interface{}, error) {
	endpoints, err := c.GetEndpoints()
	if err != nil {
		return "", err
	}
	var connString string
	for _, publicPort := range endpoints.GetPublicPorts(5672) {
		connString = fmt.Sprintf("amqp://admin:root@%s:%d", endpoints.GetHost(), publicPort)
		conn, err := amqp.Dial(connString)
		if err == nil {
			_ = conn.Close()
			break
		}
		logrus.Infof("rabbitmq connection \"%s\" failed with %s. trying another if available.", connString, err.Error())
		connString = ""
	}
	if connString == "" {
		return nil, fmt.Errorf("no valid rabbitmq connection could be establised")
	}
	return []string{connString, "amqp://admin:root@rabbitmq:5672"}, nil
}

func GetRedisConnectionConfig(c *docker.Container) (interface{}, error) {
	endpoints, err := c.GetEndpoints()
	if err != nil {
		return "", err
	}
	var connString string
	for _, publicPort := range endpoints.GetPublicPorts(6379) {
		connString = fmt.Sprintf("redis://%s:%d", endpoints.GetHost(), publicPort)
		conn, err := redis.DialURL(connString)
		if err == nil {
			_ = conn.Close()
			break
		}
		logrus.Infof("redis connection \"%s\" failed with %s. trying another if available.", connString, err.Error())
		connString = ""
	}
	if connString == "" {
		return nil, fmt.Errorf("no valid redis connection could be establised")
	}
	return []string{connString, "redis://redis:6379"}, nil
}

func GetCeleryClient(brokerUrl string, backendUrl string) (*gocelery.CeleryClient, error) {
	backend, err := getCeleryBackend(backendUrl)
	if err != nil {
		return nil, err
	}
	broker, err := getCeleryBroker(brokerUrl)
	if err != nil {
		return nil, err
	}
	return gocelery.NewCeleryClient(broker, backend, 2)
}

//============================== helpers ==========================================

func getCeleryBroker(url string) (gocelery.CeleryBroker, error) {
	if strings.HasPrefix(url, "redis://") {
		redisPool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				c, err := redis.DialURL(url)
				if err != nil {
					return nil, err
				}
				return c, err
			},
		}
		return &gocelery.RedisCeleryBroker{Pool: redisPool}, nil
	} else if strings.HasPrefix(url, "amqp://") {
		return gocelery.NewAMQPCeleryBroker(&gocelery.AMQPBrokerConfig{
			URL: url,
			ConnectionConfig: &amqp.Config{
				Heartbeat: 60 * time.Second,
			},
		})
	} else {
		return nil, fmt.Errorf("bad url scheme")
	}
}

func getCeleryBackend(url string) (gocelery.CeleryBackend, error) {
	if strings.HasPrefix(url, "redis://") {
		redisPool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				c, err := redis.DialURL(url)
				if err != nil {
					return nil, err
				}
				return c, err
			},
		}
		return &gocelery.RedisCeleryBackend{Pool: redisPool}, nil
	} else if strings.HasPrefix(url, "amqp://") {
		connection, err := amqp.DialConfig(url, amqp.Config{
			Heartbeat: 60 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		channel, err := connection.Channel()
		if err != nil {
			return nil, err
		}
		return gocelery.NewAMQPCeleryBackendByConnAndChannel(connection, channel), nil
	} else {
		return nil, fmt.Errorf("bad url scheme")
	}
}
