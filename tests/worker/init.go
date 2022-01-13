package worker

import (
	"fmt"
	"github.com/pnongah/gocelery"
	"testing"
	"tests/config"
	"tests/util"

	"github.com/sirupsen/logrus"
)

//Queues

const GoQueue = "go_queue"
const PyQueue = "py_queue"

func RunGoWorker(t *testing.T, brokerUrl string, backendUrl string) error {
	cli, err := config.GetCeleryClient(brokerUrl, backendUrl)
	if err != nil {
		return err
	}
	RegisterGoFunctions(cli)
	cli.StartWorker()
	logrus.Println("Go-worker started")
	t.Cleanup(cli.StopWorker)
	return nil
}

func RegisterGoFunctions(cli *gocelery.CeleryClient) {
	cli.Register(GoFunc_Add, &gocelery.CeleryTaskConfig{Task: Add, Queue: GoQueue})
	cli.Register(GoFuncKwargs_Add, &gocelery.CeleryTaskConfig{Task: &adder{}, Queue: GoQueue})
	cli.Register(GoFunc_Error, &gocelery.CeleryTaskConfig{Task: ThrowError, Queue: GoQueue})
	cli.Register(GoFuncKwargs_Error, &gocelery.CeleryTaskConfig{Task: &errorThrower{}, Queue: GoQueue})
}

func RunPythonWorker(t *testing.T, args ...string) error {
	return util.RunPython(t, true, func(msg string) {
		fmt.Printf("[[python-worker]] %s\n", msg)
	}, "worker/init.py", args...)
}
