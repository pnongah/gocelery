module github.com/pnongah/gocelery

go 1.16

require (
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/sirupsen/logrus v1.8.1
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace github.com/gocelery/gocelery => ./
