package main

import (
	"github.com/sirupsen/logrus"
	"os"
	"tests/config"
	"tests/worker"
)

// intended to spin up a worker that is standalone (Dockerfile-based)

func main() {
	brokerUrl := os.Args[1]
	backendUrl := os.Args[2]
	cli, err := config.GetCeleryClient(
		brokerUrl,
		backendUrl,
	)
	if err != nil {
		logrus.Fatal(err)
	}
	worker.RegisterGoFunctions(cli)
	cli.StartWorker()
	logrus.Println("Remote Go-worker started")
	cli.WaitForStopWorker()
}
