package tests

import (
	"testing"
	"tests/config"
	"tests/worker"
	"time"

	"github.com/pnongah/gocelery"

	"github.com/keon94/go-compose/docker"
	"github.com/stretchr/testify/require"
)

func TestAtlasWatcher_FullRedis(t *testing.T) {
	env := docker.StartEnvironment(config.Env,
		&docker.ServiceEntry{
			Name:    "redis",
			Handler: config.GetRedisConnectionConfig,
		},
	)
	t.Cleanup(env.Shutdown)
	redisConn := env.Services["redis"].(string)

	gocli, pycli := startWorkers(t, &config.WorkerConfig{
		UsePyWorker:  true,
		UseGoWorker:  true,
		GoBrokerURL:  redisConn + "/0",
		GoBackendURL: redisConn + "/0",
		PyBrokerURL:  redisConn + "/1",
		PyBackendURL: redisConn + "/1",
	})

	t.Run("happy path", func(t *testing.T) {
		runHappyPath(t, gocli, pycli)
	})
	t.Run("worker error", func(t *testing.T) {
		runWorkerError(t, gocli, pycli)
	})
}

func TestAtlasWorker_RabbitBrokerRedisBackend(t *testing.T) {
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
	redisConn := env.Services["redis"].(string)
	rabbitConn := env.Services["rabbitmq"].(string)

	gocli, pycli := startWorkers(t, &config.WorkerConfig{
		UseGoWorker:  true,
		UsePyWorker:  true,
		GoBrokerURL:  rabbitConn + "//go-worker",
		GoBackendURL: redisConn + "/0",
		PyBrokerURL:  rabbitConn + "//py-worker",
		PyBackendURL: redisConn + "/1",
	})
	t.Run("happy path", func(t *testing.T) {
		runHappyPath(t, gocli, pycli)
	})
	t.Run("worker error", func(t *testing.T) {
		runWorkerError(t, gocli, pycli)
	})
}

func runHappyPath(t *testing.T, gocli *gocelery.CeleryClient, pycli *gocelery.CeleryClient) {
	delay, err := gocli.Delay(worker.GoFunc_Add, 1, 2)
	require.NoError(t, err)
	result, err := gocli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, 3.0, result)
	delay, err = gocli.DelayKwargs(worker.GoFuncKwargs_Add, map[string]interface{}{
		"x": 1, "y": 2,
	})
	require.NoError(t, err)
	result, err = gocli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, 3.0, result)

	delay, err = pycli.Delay(worker.PyFunc_Sub, 2, 1)
	require.NoError(t, err)
	result, err = pycli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, 1.0, result)
}

func runWorkerError(t *testing.T, gocli *gocelery.CeleryClient, pycli *gocelery.CeleryClient) {
	expectedError := &gocelery.TaskResultError{}

	delay, err := gocli.Delay(worker.GoFunc_Error)
	require.NoError(t, err)
	_, err = gocli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
	require.ErrorAs(t, err, &expectedError)
	delay, err = gocli.DelayKwargs(worker.GoFuncKwargs_Error, nil)
	require.NoError(t, err)
	_, err = gocli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
	require.ErrorAs(t, err, &expectedError)

	delay, err = pycli.Delay(worker.PyFunc_Error)
	require.NoError(t, err)
	_, err = pycli.GetAsyncResult(delay.TaskID).Get(5 * time.Second)
	require.ErrorAs(t, err, &expectedError)
}

func startWorkers(t *testing.T, cfg *config.WorkerConfig) (gocli *gocelery.CeleryClient, pycli *gocelery.CeleryClient) {
	var err error
	if cfg.UseGoWorker {
		gocli, err = config.GetCeleryClient(cfg.GoBrokerURL, cfg.GoBackendURL)
		require.NoError(t, err)
		require.NoError(t, worker.RunGoWorker(t, cfg.GoBrokerURL, cfg.GoBackendURL))
	}
	if cfg.UsePyWorker {
		pycli, err = config.GetCeleryClient(cfg.PyBrokerURL, cfg.PyBackendURL)
		require.NoError(t, err)
		require.NoError(t, worker.RunPythonWorker(t, cfg.PyBrokerURL, cfg.PyBackendURL))
	}
	return gocli, pycli
}
