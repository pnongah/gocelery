package config

type (
	WorkerConfig struct {
		UsePyWorker       bool
		UseGoWorker       bool
		UseDocker         bool
		BrokerURL         string
		BackendURL        string
		PrivateBrokerURL  string
		PrivateBackendURL string
	}
)
