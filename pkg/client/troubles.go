package client

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ContainerManager struct {
	containers                    []string
	restartBatchSize              int
	lastCheckAllContainersHealthy bool
}

func initLogger() {
	format := strings.ToLower(os.Getenv("LOG_FORMAT"))
	zerolog.TimeFieldFormat = time.RFC3339

	var logger zerolog.Logger
	switch format {
	case "json":
		logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	default:
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	}

	levelStr := strings.ToLower(os.Getenv("MY_LOG_LEVEL"))
	level, err := zerolog.ParseLevel(levelStr)
	if err != nil {
		level = zerolog.DebugLevel
	}

	zerolog.SetGlobalLevel(level)
	log.Logger = logger
}

func NewContainerManager() (*ContainerManager, error) {
	containersEnv := os.Getenv("CONTAINERS")
	if containersEnv == "" {
		log.Fatal().Msg("CONTAINERS environment variable is not set")
	}

	restartBatchSize := 0 // default value
	if batchSizeStr := os.Getenv("RESTART_BATCH_SIZE"); batchSizeStr != "" {
		if size, err := strconv.Atoi(batchSizeStr); err == nil && size > 0 {
			restartBatchSize = size
		}
	}

	return &ContainerManager{
		containers:       strings.Split(containersEnv, ","),
		restartBatchSize: restartBatchSize,
	}, nil
}

func (cm *ContainerManager) MonitorAndRestart(ctx context.Context) {
	if cm.restartBatchSize == 0 {
		log.Info().Msg("restart batch_size = 0, returning")
	}
	initLogger()
	log.Info().
		Strs("containers", cm.containers).
		Int("restart_batch_size", cm.restartBatchSize).
		Msg("Starting container monitor")

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Context canceled, stopping monitor")
			return
		case <-ticker.C:
			restarted := cm.checkAndRestartContainers()
			if restarted {
				continue
			}
		}
	}
}

func (cm *ContainerManager) checkAndRestartContainers() bool {
	if cm.lastCheckAllContainersHealthy {
		cm.lastCheckAllContainersHealthy = false
		containersToRestart := cm.selectContainersToRestart(cm.containers)
		cm.restartContainers(containersToRestart)
		return true
	}

	failedContainers := cm.getFailedContainers()
	if len(failedContainers) == 0 {
		log.Debug().Msg("All containers are healthy")
		cm.lastCheckAllContainersHealthy = true
		return false
	}

	return false
}

func (cm *ContainerManager) getFailedContainers() []string {
	var failed []string

	for _, container := range cm.containers {
		container = strings.TrimSpace(container)
		if !cm.isContainerRunning(container) {
			failed = append(failed, container)
		}
	}

	return failed
}

func (cm *ContainerManager) isContainerRunning(container string) bool {
	cmd := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container)
	out, err := cmd.Output()
	running := strings.TrimSpace(string(out)) == "true"

	log.Debug().
		Str("container", container).
		Bool("running", running).
		Err(err).
		Msg("Health check result")

	return err == nil && running
}

func (cm *ContainerManager) selectContainersToRestart(originalContainers []string) []string {
	if len(originalContainers) == 0 {
		log.Info().Msg("No containers provided for restart selection")
		return nil
	}

	containers := make([]string, len(originalContainers))
	copy(containers, originalContainers)

	var leaderContainer string
	if strings.HasPrefix(containers[0], "etcd") {
		log.Info().Msg("Detected etcd container group, trying to identify etcd leader")

		for _, container := range containers {
			log.Info().Str("container", container).Msg("Checking if container is etcd leader")

			cmd := exec.Command("sh", "-c",
				fmt.Sprintf(`docker exec %s etcdctl --endpoints=http://localhost:2379 endpoint status --write-out=json | jq -r '.[].Status.leader == .[].Status.header.member_id'`, container),
			)

			output, err := cmd.Output()
			if err != nil {
				log.Warn().Err(err).Str("container", container).Msg("Failed to execute etcdctl")
				continue
			}

			isLeader := strings.TrimSpace(string(output)) == "true"
			if isLeader {
				leaderContainer = container
				log.Info().Str("leader", leaderContainer).Msg("Etcd leader identified")
				break
			}
		}
	}

	rand.Shuffle(len(containers), func(i, j int) {
		containers[i], containers[j] = containers[j], containers[i]
	})

	if leaderContainer != "" {
		found := false
		i := 0
		for i < cm.restartBatchSize && i < len(containers) {
			if containers[i] == leaderContainer {
				found = true
				break
			}
			i++
		}

		if found != true {
			containers[0] = leaderContainer
		}
	}

	if cm.restartBatchSize > len(containers) {
		return containers
	}

	return containers[:cm.restartBatchSize]
}

func (cm *ContainerManager) restartContainers(containers []string) {
	var wg sync.WaitGroup

	log.Info().Strs("containers", containers).Msg("going to restart")

	for _, c := range containers {
		wg.Add(1)
		go func(container string) {
			defer wg.Done()

			log.Info().Str("container", container).Msg("Attempting to restart container")
			cmd := exec.Command("docker", "restart", container)
			if out, err := cmd.CombinedOutput(); err != nil {
				log.Error().
					Str("container", container).
					Err(err).
					Msgf("Failed to restart container: %s", string(out))
			} else {
				log.Info().
					Str("container", container).
					Msg("Container restarted successfully")
			}
		}(c)
	}

	wg.Wait()
}
