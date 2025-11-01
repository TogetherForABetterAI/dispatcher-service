package server

import (
	"context"
	"sync"
	"time"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/sirupsen/logrus"
)

// ShutdownRequester defines the interface for requesting a shutdown (instead of executing it).
type ShutdownRequester interface {
	RequestShutdown()
}

type ReplicaMonitor struct {
	logger          *logrus.Logger
	config          config.Interface
	shutdownChan    chan<- struct{} // Channel to SEND the shutdown request
	stateMutex      sync.Mutex
	activeWorkers   int
	minThreshold    int
	minThresholdMet bool
	isLeader        bool
	wg              sync.WaitGroup
	startupTimer    *time.Timer
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewReplicaMonitor creates a new instance of ReplicaMonitor.
func NewReplicaMonitor(cfg config.Interface, logger *logrus.Logger, shutdownReqChan chan<- struct{}) *ReplicaMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &ReplicaMonitor{
		logger:          logger,
		config:          cfg,
		shutdownChan:    shutdownReqChan, // Assign the channel
		activeWorkers:   0,
		minThreshold:    cfg.GetMinThreshold(),
		minThresholdMet: false,
		isLeader:        cfg.IsLeader(),
		ctx:             ctx,
		cancel:          cancel,
		wg:              sync.WaitGroup{},
	}
}

// Start begins the monitoring process of ReplicaMonitor.
func (m *ReplicaMonitor) Start() {
	if m.isLeader {
		m.logger.Info("Replica Monitor: Leader detected. Monitoring not required.")
		return
	}

	timeoutDuration := m.config.GetStartupTimeout()
	m.logger.WithFields(logrus.Fields{
		"threshold": m.minThreshold,
		"timeout":   timeoutDuration,
	}).Info("Replica Monitor: Non-leader started. Activating startup timer.")

	m.startupTimer = time.NewTimer(timeoutDuration)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		select {
		case <-m.startupTimer.C:
			// Timer fired
			m.stateMutex.Lock()
			if !m.minThresholdMet {
				// Threshold not met in time
				m.logger.WithField("timeout", timeoutDuration).Warn("Replica Monitor: Startup timer EXPIRED. Requesting shutdown.")
				// Request shutdown
				go m.requestShutdown()
			} else {
				m.logger.Debug("Replica Monitor: Timer fired, but threshold already met.")
			}
			m.stateMutex.Unlock()

		case <-m.ctx.Done():
			// Graceful shutdown requested
			m.logger.Debug("Replica Monitor: Shutdown requested. Stopping startup timer.")
			if !m.startupTimer.Stop() {
				<-m.startupTimer.C // just for cleanup
			}
			return
		}
	}()
}

// NotifyWorkerStart is called when a worker starts a task.
func (m *ReplicaMonitor) NotifyWorkerStart() {
	m.stateMutex.Lock()
	defer m.stateMutex.Unlock()

	m.activeWorkers++
	m.logger.WithField("active_workers", m.activeWorkers).Debug("Replica Monitor: Worker started a task")

	if !m.isLeader && !m.minThresholdMet && m.activeWorkers >= m.minThreshold {
		m.logger.WithFields(logrus.Fields{
			"active_workers": m.activeWorkers,
			"threshold":      m.minThreshold,
		}).Info("Replica Monitor: Minimum threshold REACHED. Stopping startup timer.")

		m.minThresholdMet = true
		if m.startupTimer != nil {
			if !m.startupTimer.Stop() {
				select {
				case <-m.startupTimer.C:
				default:
				}
			}
		}
	}
}

// NotifyWorkerFinish is called when a worker finishes a task.
func (m *ReplicaMonitor) NotifyWorkerFinish() {
	m.stateMutex.Lock()
	defer m.stateMutex.Unlock()

	m.activeWorkers--
	m.logger.WithField("active_workers", m.activeWorkers).Debug("Replica Monitor: Worker finished a task")

	if !m.isLeader && !m.minThresholdMet && m.activeWorkers < m.minThreshold {
		m.logger.WithFields(logrus.Fields{
			"active_workers": m.activeWorkers,
			"threshold":      m.minThreshold,
		}).Warn("Replica Monitor: Active workers FELL BELOW threshold. Requesting shutdown.")

		// Request shutdown instead of calling it directly
		go m.requestShutdown()
	}
}

// requestShutdown sends a signal to the shutdown channel in a non-blocking way.
func (m *ReplicaMonitor) requestShutdown() {
	select {
	case m.shutdownChan <- struct{}{}:
		m.logger.Info("Replica Monitor: Shutdown request sent.")
	default:
	}
}

// Stop (No changes, still called by Server.Stop())
func (m *ReplicaMonitor) Stop() {
	m.logger.Info("Replica Monitor: Stopping monitor.")
	m.cancel()  // Signals the timer goroutine
	m.wg.Wait() // Waits for the timer goroutine to finish
}
