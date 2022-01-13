package config

type (
	WorkerConfig struct {
		UsePyWorker bool
		UseGoWorker bool
		BrokerURL   string
		BackendURL  string
	}
)
