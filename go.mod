module github.com/pnongah/gocelery

go 1.16

require (
	github.com/gomodule/redigo v1.8.8
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/sirupsen/logrus v1.8.1
	github.com/streadway/amqp v1.0.0
	golang.org/x/sys v0.0.0-20220114195835-da31bd327af9 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace github.com/gocelery/gocelery => ./
