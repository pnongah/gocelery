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

	cli := startWorkers(t, &config.WorkerConfig{
		UsePyWorker: true,
		UseGoWorker: true,
		BrokerURL:   redisConn + "/0",
		BackendURL:  redisConn + "/0",
	})

	t.Run("happy path", func(t *testing.T) {
		runHappyPath(t, cli)
	})
	t.Run("worker error", func(t *testing.T) {
		runWorkerError(t, cli)
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

	cli := startWorkers(t, &config.WorkerConfig{
		UseGoWorker: true,
		UsePyWorker: true,
		BrokerURL:   rabbitConn + "//worker",
		BackendURL:  redisConn + "/0",
	})
	t.Run("happy path", func(t *testing.T) {
		runHappyPath(t, cli)
	})
	t.Run("worker error", func(t *testing.T) {
		runWorkerError(t, cli)
	})
}

func runHappyPath(t *testing.T, cli *gocelery.CeleryClient) {
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

func runWorkerError(t *testing.T, cli *gocelery.CeleryClient) {
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

func startWorkers(t *testing.T, cfg *config.WorkerConfig) (gocli *gocelery.CeleryClient) {
	var err error
	gocli, err = config.GetCeleryClient(cfg.BrokerURL, cfg.BackendURL)
	require.NoError(t, err)
	if cfg.UseGoWorker {
		require.NoError(t, worker.RunGoWorker(t, cfg.BrokerURL, cfg.BackendURL))
	}
	if cfg.UsePyWorker {
		require.NoError(t, worker.RunPythonWorker(t, cfg.BrokerURL, cfg.BackendURL))
	}
	return gocli
}
