package util

import (
	"fmt"
	"github.com/keon94/go-compose/docker"
	"os/exec"
	"testing"
)

// PythonBin the name of the python binary on the system
const PythonBin = "python3"

func RunPython(t *testing.T, async bool, logger func(msg string), file string, args ...string) error {
	pyargs := []string{file}
	pyargs = append(pyargs, args...)
	cmd := exec.Command(PythonBin, pyargs...)
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	if err := docker.RunProcessWithLogs(cmd, logger); err != nil {
		return fmt.Errorf("could not start python process: %w", err)
	}
	if !async {
		fmt.Println("Waiting for python process to complete")
		err := cmd.Wait()
		fmt.Println("Python process ended")
		return err
	}
	return nil
}
