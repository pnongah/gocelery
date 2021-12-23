package worker

import (
	"fmt"
	"os/exec"
	"testing"
	"tests/config"

	"github.com/keon94/go-compose/docker"

	"github.com/pnongah/gocelery"

	"github.com/sirupsen/logrus"
)

// PythonBin the name of the python binary on the system
const PythonBin = "python3"

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
	cli.Register(GoFunc_Add, Add)
	cli.Register(GoFuncKwargs_Add, &adder{})
	cli.Register(GoFunc_Error, ThrowError)
	cli.Register(GoFuncKwargs_Error, &errorThrower{})
}

func RunPythonWorker(t *testing.T, args ...string) error {
	pyargs := []string{"worker/main.py"}
	pyargs = append(pyargs, args...)
	cmd := exec.Command(PythonBin, pyargs...)
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	if err := docker.RunProcessWithLogs(cmd, func(msg string) {
		fmt.Printf("[[python-worker]] %s\n", msg)
	}); err != nil {
		return fmt.Errorf("could not start python worker: %w", err)
	}
	return nil
}
