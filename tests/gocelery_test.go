package tests

import (
	"fmt"
	"runtime"
	"testing"
	"tests/config"
	"tests/util"
	"tests/worker"
	"time"

	"github.com/pnongah/gocelery"
	"github.com/sirupsen/logrus"

	"github.com/keon94/go-compose/docker"
	"github.com/stretchr/testify/require"
)

const useDocker = true

var dockerHostName string

func init() {
	logrus.SetLevel(logrus.DebugLevel)
	dockerHostName = "host.docker.internal"
	if runtime.GOOS == "windows" {
		dockerHostName = "docker.for.win.localhost"
	}
}

func TestFullRedis(t *testing.T) {
	env := docker.StartEnvironment(config.Env,
		&docker.ServiceEntry{
			Name:    "redis",
			Handler: config.GetRedisConnectionConfig,
		},
	)
	t.Cleanup(env.Shutdown)
	redisConns := env.Services["redis"].([]string)
	cfg := &config.WorkerConfig{
		UsePyWorker:       true,
		UseGoWorker:       true,
		UseDocker:         useDocker,
		BrokerURL:         redisConns[0] + "/0",
		BackendURL:        redisConns[0] + "/1",
		PrivateBrokerURL:  redisConns[1] + "/0",
		PrivateBackendURL: redisConns[1] + "/1",
	}
	cli := startWorkers(t, env, cfg)
	t.Run("go-client happy path", func(t *testing.T) {
		runGoClientHappyPath(t, cli)
	})
	t.Run("go-client worker error", func(t *testing.T) {
		runGoClientWorkerError(t, cli)
	})
	t.Run("py-client tests", func(t *testing.T) {
		runPythonTests(t, cfg)
	})
}

func TestRabbitBrokerRedisBackend(t *testing.T) {
	env := docker.StartEnvironment(config.Env,
		&docker.ServiceEntry{
			Name:    "redis",
			Handler: config.GetRedisConnectionConfig,
		},
		&docker.ServiceEntry{
			Name:    "rabbitmq",
			Handler: config.GetRabbitMQConnectionConfig,
		},
	)
	t.Cleanup(env.Shutdown)
	redisConns := env.Services["redis"].([]string)
	rabbitConns := env.Services["rabbitmq"].([]string)

	cfg := &config.WorkerConfig{
		UseGoWorker:       true,
		UsePyWorker:       true,
		UseDocker:         useDocker,
		BrokerURL:         rabbitConns[0] + "//worker",
		BackendURL:        redisConns[0] + "/0",
		PrivateBrokerURL:  rabbitConns[1] + "//worker",
		PrivateBackendURL: redisConns[1] + "/0",
	}
	cli := startWorkers(t, env, cfg)
	t.Run("go-client happy path", func(t *testing.T) {
		runGoClientHappyPath(t, cli)
	})
	t.Run("go-client worker error", func(t *testing.T) {
		runGoClientWorkerError(t, cli)
	})
	t.Run("py-client tests", func(t *testing.T) {
		runPythonTests(t, cfg)
	})
}

func runPythonTests(t *testing.T, cfg *config.WorkerConfig, testCases ...string) {
	args := []string{cfg.BrokerURL, cfg.BackendURL}
	args = append(args, testCases...)
	err := util.RunPython(t, false, func(msg string) {
		fmt.Printf("[[python-test]] %s\n", msg)
	}, "gocelery_test.py", args...)
	require.NoError(t, err)
}

func runGoClientHappyPath(t *testing.T, cli *gocelery.CeleryClient) {
	{
		delay, err := cli.Delay(worker.GoFunc_Add, &gocelery.TaskParameters{
			Args:  []interface{}{1, 2},
			Queue: worker.GoQueue,
		})
		require.NoError(t, err)
		result, err := cli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
		require.NoError(t, err)
		require.Equal(t, 3.0, result)
		delay, err = cli.Delay(worker.GoFuncKwargs_Add, &gocelery.TaskParameters{
			Kwargs: map[string]interface{}{"x": 1, "y": 2},
			Queue:  worker.GoQueue,
		})
		require.NoError(t, err)
		result, err = cli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
		require.NoError(t, err)
		require.Equal(t, 3.0, result)
	}
	{
		delay, err := cli.Delay(worker.PyFunc_Sub, &gocelery.TaskParameters{
			Args:  []interface{}{2, 1},
			Queue: worker.PyQueue,
		})
		require.NoError(t, err)
		result, err := cli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
		require.NoError(t, err)
		require.Equal(t, 1.0, result)
	}
}

func runGoClientWorkerError(t *testing.T, cli *gocelery.CeleryClient) {
	expectedError := &gocelery.TaskResultError{}
	{
		delay, err := cli.Delay(worker.GoFunc_Error, &gocelery.TaskParameters{
			Queue: worker.GoQueue,
		})
		require.NoError(t, err)
		_, err = cli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
		require.ErrorAs(t, err, &expectedError)
		delay, err = cli.Delay(worker.GoFuncKwargs_Error, &gocelery.TaskParameters{
			Queue: worker.GoQueue,
		})
		require.NoError(t, err)
		_, err = cli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
		require.ErrorAs(t, err, &expectedError)
	}
	{
		delay, err := cli.Delay(worker.PyFunc_Error, nil)
		require.NoError(t, err)
		_, err = cli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
		require.ErrorAs(t, err, &expectedError)
	}
}

func startWorkers(t *testing.T, env *docker.Environment, cfg *config.WorkerConfig) (cli *gocelery.CeleryClient) {
	cli, err := config.GetCeleryClient(cfg.BrokerURL, cfg.BackendURL)
	require.NoError(t, err)
	if cfg.UseGoWorker {
		if cfg.UseDocker {
			err = env.StartServices(
				&docker.ServiceEntry{
					Name: "go-worker",
					EnvironmentVars: map[string]string{
						"CELERY_BROKER":  cfg.PrivateBrokerURL,
						"CELERY_BACKEND": cfg.PrivateBackendURL,
						"HOSTNAME":       dockerHostName,
					},
				})
		} else {
			err = worker.RunGoWorker(t, cfg.BrokerURL, cfg.BackendURL)
		}
	}
	require.NoError(t, err)
	if cfg.UsePyWorker {
		if cfg.UseDocker {
			err = env.StartServices(
				&docker.ServiceEntry{
					Name: "py-worker",
					EnvironmentVars: map[string]string{
						"CELERY_BROKER":  cfg.PrivateBrokerURL,
						"CELERY_BACKEND": cfg.PrivateBackendURL,
						"HOSTNAME":       dockerHostName,
					},
				})
		} else {
			err = worker.RunPythonWorker(t, cfg.BrokerURL, cfg.BackendURL)
		}
	}
	require.NoError(t, err)
	return cli
}
