package server

import (
	"sync"
	"testing"
	"time"

	"github.com/data-dispatcher-service/src/config"
	"github.com/sirupsen/logrus"
)

// fakeConfig implements config.Interface for testing
type fakeConfig struct {
	logLevel       string
	replicaName    string
	consumerTag    string
	workerPoolSize int
	isLeader       bool
	minThreshold   int
	startupTimeout time.Duration
}

func (f *fakeConfig) GetLogLevel() string                           { return f.logLevel }
func (f *fakeConfig) GetReplicaName() string                        { return f.replicaName }
func (f *fakeConfig) GetConsumerTag() string                        { return f.consumerTag }
func (f *fakeConfig) GetMiddlewareConfig() *config.MiddlewareConfig { return nil }
func (f *fakeConfig) GetGrpcConfig() *config.GrpcConfig             { return nil }
func (f *fakeConfig) GetWorkerPoolSize() int                        { return f.workerPoolSize }
func (f *fakeConfig) IsLeader() bool                                { return f.isLeader }
func (f *fakeConfig) GetMinThreshold() int                          { return f.minThreshold }
func (f *fakeConfig) GetStartupTimeout() time.Duration              { return f.startupTimeout }

// fakeOrchestrator implements Orchestrator for testing
type fakeOrchestrator struct {
	mu                 sync.Mutex
	shutdownCalls      int
	scaleUpCalls       int
	shutdownNotifyChan chan struct{}
	scaleUpNotifyChan  chan struct{}
	shutdownChan       chan struct{} // The channel returned by RequestShutdown
}

func newFakeOrchestrator() *fakeOrchestrator {
	return &fakeOrchestrator{
		shutdownNotifyChan: make(chan struct{}, 10),
		scaleUpNotifyChan:  make(chan struct{}, 10),
		shutdownChan:       make(chan struct{}, 1),
	}
}

func (f *fakeOrchestrator) RequestShutdown() {
	f.mu.Lock()
	f.shutdownCalls++
	f.mu.Unlock()

	select {
	case f.shutdownNotifyChan <- struct{}{}:
	default:
	}
}

func (f *fakeOrchestrator) RequestScaleUp() {
	f.mu.Lock()
	f.scaleUpCalls++
	f.mu.Unlock()

	select {
	case f.scaleUpNotifyChan <- struct{}{}:
	default:
	}
}

func (f *fakeOrchestrator) getShutdownCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shutdownCalls
}

func (f *fakeOrchestrator) getScaleUpCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scaleUpCalls
}

func (f *fakeOrchestrator) waitForShutdown(timeout time.Duration) bool {
	select {
	case <-f.shutdownNotifyChan:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (f *fakeOrchestrator) waitForScaleUp(timeout time.Duration) bool {
	select {
	case <-f.scaleUpNotifyChan:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Helper function to create a logger that discards output
func createTestLogger(t *testing.T) *logrus.Logger {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(logrus.StandardLogger().Out)
	logger.SetLevel(logrus.ErrorLevel) // Minimize test output
	return logger
}

func TestReplicaMonitor_Start_AsLeader(t *testing.T) {
	t.Parallel()

	t.Run("leader does not start timer or invoke shutdown", func(t *testing.T) {
		cfg := &fakeConfig{
			isLeader:       true,
			minThreshold:   2,
			workerPoolSize: 5,
			startupTimeout: 5 * time.Millisecond,
		}
		logger := createTestLogger(t)
		orch := newFakeOrchestrator()

		monitor := NewReplicaMonitor(cfg, logger, orch)
		monitor.Start()

		// Use a channel-based timeout to verify no shutdown is called
		// Wait longer than the startup timeout to ensure timer would have fired if started
		select {
		case <-orch.shutdownNotifyChan:
			t.Fatal("unexpected shutdown call for leader")
		case <-time.After(20 * time.Millisecond):
			// Success: no shutdown was called
		}

		// Stop the monitor (for symmetry, though timer never started for leader)
		monitor.Stop()

		// Verify no shutdown was called
		if orch.getShutdownCalls() != 0 {
			t.Fatalf("expected 0 shutdown calls, got %d", orch.getShutdownCalls())
		}
	})
}

func TestReplicaMonitor_Start_TimerExpiresWithoutMeetingThreshold(t *testing.T) {
	t.Parallel()
	t.Run("timer expires and RequestShutdown is called", func(t *testing.T) {
		cfg := &fakeConfig{
			isLeader:       false,
			minThreshold:   2,
			workerPoolSize: 5,
			startupTimeout: 20 * time.Millisecond,
		}
		logger := createTestLogger(t)
		orch := newFakeOrchestrator()

		monitor := NewReplicaMonitor(cfg, logger, orch)
		monitor.Start()

		t.Cleanup(func() {
			monitor.Stop()
		})

		// Wait for shutdown to be called
		if !orch.waitForShutdown(100 * time.Millisecond) {
			t.Fatal("expected RequestShutdown to be called after timer expiration")
		}

		// Verify exactly one shutdown call
		if calls := orch.getShutdownCalls(); calls != 1 {
			t.Fatalf("expected exactly 1 shutdown call, got %d", calls)
		}
	})
}

func TestReplicaMonitor_Start_ThresholdMetBeforeTimeout(t *testing.T) {
	t.Parallel()
	t.Run("worker reaches minThreshold before timer expires", func(t *testing.T) {
		cfg := &fakeConfig{
			isLeader:       false,
			minThreshold:   1,
			workerPoolSize: 5,
			startupTimeout: 200 * time.Millisecond,
		}
		logger := createTestLogger(t)
		orch := newFakeOrchestrator()

		monitor := NewReplicaMonitor(cfg, logger, orch)
		monitor.Start()

		t.Cleanup(func() {
			monitor.Stop()
		})

		// Notify that a worker started (meets threshold)
		monitor.NotifyWorkerStart()

		// Use channel-based wait instead of sleep to verify no shutdown is called
		// Wait longer than the startup timeout to ensure timer would have expired if not stopped
		select {
		case <-orch.shutdownNotifyChan:
			t.Fatal("unexpected shutdown call after meeting threshold")
		case <-time.After(250 * time.Millisecond):
			// Success: no shutdown was called
		}

		// Verify minThresholdMet is true
		monitor.stateMutex.Lock()
		met := monitor.minThresholdMet
		monitor.stateMutex.Unlock()

		if !met {
			t.Fatal("expected minThresholdMet to be true after reaching threshold")
		}
	})
}

func TestReplicaMonitor_NotifyWorkerStart_TriggersScaleUpAtMaxThreshold(t *testing.T) {
	t.Parallel()
	t.Run("RequestScaleUp is called when maxThreshold is reached", func(t *testing.T) {
		cfg := &fakeConfig{
			isLeader:       false,
			minThreshold:   1,
			workerPoolSize: 3,
			startupTimeout: 100 * time.Millisecond,
		}
		logger := createTestLogger(t)
		orch := newFakeOrchestrator()

		monitor := NewReplicaMonitor(cfg, logger, orch)
		monitor.Start()

		t.Cleanup(func() {
			monitor.Stop()
		})

		// Meet min threshold first
		monitor.NotifyWorkerStart()

		// Add workers to reach maxThreshold (total 3)
		monitor.NotifyWorkerStart()
		monitor.NotifyWorkerStart()

		// Wait for scale-up to be called
		if !orch.waitForScaleUp(100 * time.Millisecond) {
			t.Fatal("expected RequestScaleUp to be called when maxThreshold is reached")
		}

		// Verify exactly one scale-up call
		if calls := orch.getScaleUpCalls(); calls != 1 {
			t.Fatalf("expected exactly 1 scale-up call, got %d", calls)
		}

		// Additional worker starts should not trigger another scale-up
		monitor.NotifyWorkerStart()

		// Use channel-based wait to verify no additional scale-up calls
		select {
		case <-orch.scaleUpNotifyChan:
			t.Fatal("unexpected second scale-up call")
		case <-time.After(50 * time.Millisecond):
			// Success: no additional scale-up
		}

		if calls := orch.getScaleUpCalls(); calls != 1 {
			t.Fatalf("expected still 1 scale-up call after additional workers, got %d", calls)
		}
	})
}

func TestReplicaMonitor_NotifyWorkerFinish_TriggersShutdownBelowMinThreshold(t *testing.T) {
	t.Parallel()
	t.Run("RequestShutdown is called when workers fall below minThreshold", func(t *testing.T) {
		cfg := &fakeConfig{
			isLeader:       false,
			minThreshold:   2,
			workerPoolSize: 5,
			startupTimeout: 100 * time.Millisecond,
		}
		logger := createTestLogger(t)
		orch := newFakeOrchestrator()

		monitor := NewReplicaMonitor(cfg, logger, orch)
		monitor.Start()

		t.Cleanup(func() {
			monitor.Stop()
		})

		// start one worker (1 < 2 threshold)
		monitor.NotifyWorkerStart()

		// Worker finishes, bringing count to 0 (below minThreshold, 0 < 2)
		monitor.NotifyWorkerFinish()

		// Wait for shutdown to be called
		if !orch.waitForShutdown(100 * time.Millisecond) {
			t.Fatal("expected RequestShutdown to be called when workers fall below minThreshold")
		}

		// Verify exactly one shutdown call
		if calls := orch.getShutdownCalls(); calls != 1 {
			t.Fatalf("expected exactly 1 shutdown call, got %d", calls)
		}
	})
}

func TestReplicaMonitor_NotifyWorkerFinish_ResetsMaxThresholdReached(t *testing.T) {
	t.Parallel()
	t.Run("maxThresholdReached resets when workers drop below resetThreshold", func(t *testing.T) {
		cfg := &fakeConfig{
			isLeader:       false,
			minThreshold:   1,
			workerPoolSize: 5,
			startupTimeout: 100 * time.Millisecond,
		}
		logger := createTestLogger(t)
		orch := newFakeOrchestrator()

		monitor := NewReplicaMonitor(cfg, logger, orch)
		monitor.Start()

		t.Cleanup(func() {
			monitor.Stop()
		})

		// Meet min threshold
		monitor.NotifyWorkerStart()

		// Reach maxThreshold (5 workers)
		for i := 0; i < 4; i++ {
			monitor.NotifyWorkerStart()
		}

		// Wait for first scale-up
		if !orch.waitForScaleUp(100 * time.Millisecond) {
			t.Fatal("expected first RequestScaleUp to be called")
		}

		if calls := orch.getScaleUpCalls(); calls != 1 {
			t.Fatalf("expected 1 scale-up call, got %d", calls)
		}

		// Calculate resetThreshold: int(5 * 0.7) = 3
		// Drop workers below resetThreshold (to 3)
		for i := 0; i < 2; i++ {
			monitor.NotifyWorkerFinish()
		}

		// Verify maxThresholdReached is reset
		monitor.stateMutex.Lock() // not need to lock (cause is sequential in this case) but simulates real usage
		reached := monitor.maxThresholdReached
		workers := monitor.activeWorkers
		monitor.stateMutex.Unlock()

		if workers != 3 {
			t.Fatalf("expected activeWorkers to be 3, got %d", workers)
		}

		if reached {
			t.Fatal("expected maxThresholdReached to be false after dropping below resetThreshold")
		}

		// Now add workers again to reach maxThreshold (5)
		for i := 0; i < 2; i++ {
			monitor.NotifyWorkerStart()
		}

		// Wait for second scale-up
		if !orch.waitForScaleUp(100 * time.Millisecond) {
			t.Fatal("expected second RequestScaleUp to be called after reset")
		}

		// Verify two scale-up calls total
		if calls := orch.getScaleUpCalls(); calls != 2 {
			t.Fatalf("expected 2 scale-up calls total, got %d", calls)
		}
	})
}

func TestReplicaMonitor_Stop_CancelsContextAndWaitsForGoroutine(t *testing.T) {
	t.Parallel()
	t.Run("Stop cancels context and waits for timer goroutine", func(t *testing.T) {
		cfg := &fakeConfig{
			isLeader:       false,
			minThreshold:   2,
			workerPoolSize: 5,
			startupTimeout: 500 * time.Millisecond,
		}
		logger := createTestLogger(t)
		orch := newFakeOrchestrator()

		monitor := NewReplicaMonitor(cfg, logger, orch)
		monitor.Start()

		// Brief pause to allow goroutine to start
		time.Sleep(10 * time.Millisecond)

		// Stop should cancel context and wait for goroutine
		done := make(chan struct{})
		go func() {
			monitor.Stop()
			close(done)
		}()

		// Verify Stop completes within reasonable time
		select {
		case <-done:
			// Success: Stop completed without blocking
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Stop() did not complete within expected time")
		}

		// Verify no shutdown was called (timer was cancelled before expiry)
		if calls := orch.getShutdownCalls(); calls != 0 {
			t.Fatalf("expected 0 shutdown calls after Stop, got %d", calls)
		}
	})
}

func TestReplicaMonitor_NotifyWorkerStart_AsLeader(t *testing.T) {
	t.Parallel()
	t.Run("leader does not trigger shutdown on NotifyWorkerStart", func(t *testing.T) {
		cfg := &fakeConfig{
			isLeader:       true,
			minThreshold:   2,
			workerPoolSize: 3,
			startupTimeout: 100 * time.Millisecond,
		}
		logger := createTestLogger(t)
		orch := newFakeOrchestrator()

		monitor := NewReplicaMonitor(cfg, logger, orch)
		monitor.Start()

		t.Cleanup(func() {
			monitor.Stop()
		})

		// Add workers
		monitor.NotifyWorkerStart()

		// Verify min threshold checks don't apply to leader
		monitor.stateMutex.Lock()
		met := monitor.minThresholdMet
		monitor.stateMutex.Unlock()

		if met {
			t.Fatal("leader should not set minThresholdMet")
		}

		// But max threshold should still work for leader
		monitor.NotifyWorkerStart()
		monitor.NotifyWorkerStart()

		// Wait for scale-up
		if !orch.waitForScaleUp(100 * time.Millisecond) {
			t.Fatal("expected leader to trigger scale-up at maxThreshold")
		}

		if calls := orch.getScaleUpCalls(); calls != 1 {
			t.Fatalf("expected 1 scale-up call for leader, got %d", calls)
		}
	})
}
