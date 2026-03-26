package executorclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"

	"github.com/uber/cadence/client/sharddistributorexecutor"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/client/clientcommon"
	"github.com/uber/cadence/service/sharddistributor/client/executorclient/metricsconstants"
	"github.com/uber/cadence/service/sharddistributor/client/executorclient/syncgeneric"
)

var (
	// ErrLocalPassthroughMode indicates that the heartbeat loop should stop due to local passthrough mode
	ErrLocalPassthroughMode = errors.New("local passthrough mode: stopping heartbeat loop")
	// ErrAssignmentDivergenceLocalShard indicates that the local shard is not reported back from the heartbeat
	ErrAssignmentDivergenceLocalShard = errors.New("assignment divergence: local shard not in heartbeat or not ready")
	// ErrAssignmentDivergenceHeartbeatShard indicates that the shard in the heartbeat is not present in the local assignment
	ErrAssignmentDivergenceHeartbeatShard = errors.New("assignment divergence: heartbeat shard not in local")
)

type processorState int32

const (
	processorStateStarting processorState = iota
	processorStateStarted
	processorStateStopping
)

const (
	heartbeatJitterCoeff     = 0.1 // 10% jitter
	drainingHeartbeatTimeout = 5 * time.Second
)

// processorAsyncOperationTimeout is the maximum time allowed for a shard processor
// Start or Stop call. Declared as a var so tests can override it without waiting
// the full 2 minutes.
var processorAsyncOperationTimeout = 2 * time.Minute

type managedProcessor[SP ShardProcessor] struct {
	processor SP
	state     atomic.Int32
}

type syncExecutorMetadata struct {
	sync.RWMutex

	data map[string]string
}

func (m *syncExecutorMetadata) Set(metadata map[string]string) {
	m.Lock()
	defer m.Unlock()

	m.data = metadata
}

func (m *syncExecutorMetadata) Get() map[string]string {
	m.RLock()
	defer m.RUnlock()

	// Copy the map
	result := make(map[string]string, len(m.data))
	for k, v := range m.data {
		result[k] = v
	}

	return result
}

func (mp *managedProcessor[SP]) setState(state processorState) {
	mp.state.Store(int32(state))
}

func (mp *managedProcessor[SP]) getState() processorState {
	return processorState(mp.state.Load())
}

func newManagedProcessor[SP ShardProcessor](processor SP, state processorState) *managedProcessor[SP] {
	managed := &managedProcessor[SP]{
		processor: processor,
		state:     atomic.Int32{},
	}

	managed.setState(state)
	return managed
}

type executorImpl[SP ShardProcessor] struct {
	logger                 log.Logger
	shardDistributorClient sharddistributorexecutor.Client
	shardProcessorFactory  ShardProcessorFactory[SP]
	namespace              string
	stopC                  chan struct{}
	heartBeatInterval      time.Duration
	ttlShard               time.Duration
	managedProcessors      syncgeneric.Map[string, *managedProcessor[SP]]
	processorsToLastUse    syncgeneric.Map[string, time.Time]
	executorID             string
	timeSource             clock.TimeSource
	processLoopWG          sync.WaitGroup
	assignmentMutex        sync.Mutex
	metrics                tally.Scope
	hostMetrics            tally.Scope
	migrationMode          atomic.Int32
	metadata               syncExecutorMetadata
	drainObserver          clientcommon.DrainSignalObserver
}

func (e *executorImpl[SP]) setMigrationMode(mode types.MigrationMode) {
	e.migrationMode.Store(int32(mode))
}

func (e *executorImpl[SP]) getMigrationMode() types.MigrationMode {
	return types.MigrationMode(e.migrationMode.Load())
}

func (e *executorImpl[SP]) Start(ctx context.Context) {
	e.logger.Info("starting shard distributor executor", tag.ShardNamespace(e.namespace))
	e.processLoopWG.Add(2)
	go func() {
		defer e.processLoopWG.Done()
		e.heartbeatloop(context.WithoutCancel(ctx))
	}()
	go func() {
		defer e.processLoopWG.Done()
		e.shardCleanUpLoop(context.WithoutCancel(ctx))
	}()
}

func (e *executorImpl[SP]) Stop() {
	e.logger.Info("stopping shard distributor executor", tag.ShardNamespace(e.namespace))
	close(e.stopC)
	e.processLoopWG.Wait()
}

func (e *executorImpl[SP]) GetShardProcess(ctx context.Context, shardID string) (SP, error) {
	e.processorsToLastUse.Store(shardID, e.timeSource.Now())

	shardProcess, ok := e.managedProcessors.Load(shardID)
	if !ok {
		if e.getMigrationMode() == types.MigrationModeLOCALPASSTHROUGH {
			// Fail immediately if we are in LOCAL_PASSTHROUGH mode
			var zero SP
			return zero, fmt.Errorf("%w for shard ID: %s", ErrShardProcessNotFound, shardID)
		}

		// Do a heartbeat and check again
		err := e.heartbeatAndUpdateAssignment(ctx)
		if err != nil {
			var zero SP
			return zero, fmt.Errorf("heartbeat and assign shards: %w", err)
		}

		// Check again if the shard process is found
		shardProcess, ok = e.managedProcessors.Load(shardID)
		if !ok {
			var zero SP
			return zero, fmt.Errorf("%w for shard ID: %s", ErrShardProcessNotFound, shardID)
		}
	}

	return shardProcess.processor, nil
}

func (e *executorImpl[SP]) IsOnboardedToSD() bool {
	return e.getMigrationMode() == types.MigrationModeONBOARDED
}

func (e *executorImpl[SP]) AssignShardsFromLocalLogic(ctx context.Context, shardAssignment map[string]*types.ShardAssignment) error {
	e.assignmentMutex.Lock()
	defer e.assignmentMutex.Unlock()
	if e.getMigrationMode() == types.MigrationModeONBOARDED {
		return fmt.Errorf("migration mode is onborded, no local assignemnt allowed")
	}
	e.logger.Info("Executing external shard assignment")
	e.addNewShards(ctx, shardAssignment)
	return nil
}

func (e *executorImpl[SP]) RemoveShardsFromLocalLogic(shardIDs []string) error {
	if e.getMigrationMode() == types.MigrationModeONBOARDED {
		return fmt.Errorf("migration mode is onborded, no local assignemnt allowed")
	}

	return e.removeShards(shardIDs)
}

func (e *executorImpl[SP]) removeShards(shardIDs []string) error {
	e.assignmentMutex.Lock()
	defer e.assignmentMutex.Unlock()
	e.logger.Info("Executing external shard deletion assignment")
	e.deleteShards(shardIDs)
	return nil
}

// drainChannel returns the drain signal channel, or nil if no observer is configured.
func (e *executorImpl[SP]) drainChannel() <-chan struct{} {
	if e.drainObserver != nil {
		return e.drainObserver.Drain()
	}
	return nil
}

func (e *executorImpl[SP]) heartbeatloop(ctx context.Context) {
	// Check if initial migration mode is LOCAL_PASSTHROUGH - if so, skip heartbeating entirely
	if e.getMigrationMode() == types.MigrationModeLOCALPASSTHROUGH {
		e.logger.Info("initial migration mode is local passthrough, skipping heartbeat loop")
		return
	}

	heartBeatTimer := e.timeSource.NewTimer(backoff.JitDuration(e.heartBeatInterval, heartbeatJitterCoeff))
	defer heartBeatTimer.Stop()

	drainCh := e.drainChannel()

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("shard distributor executor context done, stopping")
			e.stopShardProcessors()
			e.sendDrainingHeartbeat()
			return
		case <-e.stopC:
			e.logger.Info("shard distributor executor stopped")
			e.stopShardProcessors()
			e.sendDrainingHeartbeat()
			return
		case <-drainCh:
			e.logger.Info("drain signal received, stopping shard processors")
			e.stopShardProcessors()
			e.sendDrainingHeartbeat()

			if !e.waitForUndrain(ctx) {
				return
			}

			e.logger.Info("undrain signal received, resuming heartbeat")
			drainCh = e.drainObserver.Drain()
			heartBeatTimer.Reset(backoff.JitDuration(e.heartBeatInterval, heartbeatJitterCoeff))
		case <-heartBeatTimer.Chan():
			heartBeatTimer.Reset(backoff.JitDuration(e.heartBeatInterval, heartbeatJitterCoeff))
			err := e.heartbeatAndUpdateAssignment(ctx)
			if errors.Is(err, ErrLocalPassthroughMode) {
				e.logger.Info("local passthrough mode: stopping heartbeat loop")
				return
			}
			if err != nil {
				e.logger.Error("failed to heartbeat and assign shards", tag.Error(err))
				continue
			}
		}
	}
}

// waitForUndrain blocks until the undrain signal fires or the executor is stopped.
// Returns true if undrained (caller should resume), false if stopped.
func (e *executorImpl[SP]) waitForUndrain(ctx context.Context) bool {
	if e.drainObserver == nil {
		return false
	}

	undrainCh := e.drainObserver.Undrain()

	select {
	case <-ctx.Done():
		return false
	case <-e.stopC:
		return false
	case <-undrainCh:
		return true
	}
}

func (e *executorImpl[SP]) heartbeatAndUpdateAssignment(ctx context.Context) error {
	if !e.assignmentMutex.TryLock() {
		e.logger.Error("still doing assignment, skipping heartbeat")
		e.metrics.Counter(metricsconstants.ShardDistributorExecutorHeartbeatSkipped).Inc(1)
		return nil
	}
	defer e.assignmentMutex.Unlock()
	shardAssignment, err := e.heartbeatAndHandleMigrationMode(ctx)
	if err != nil {
		return err
	}
	if shardAssignment != nil {
		e.updateShardAssignmentMetered(ctx, shardAssignment)
	}
	return nil
}

func (e *executorImpl[SP]) heartbeatAndHandleMigrationMode(ctx context.Context) (shardAssignment map[string]*types.ShardAssignment, err error) {
	shardAssignment, migrationMode, err := e.heartbeat(ctx)
	if err != nil {
		// TODO: should we stop the executor, and drop all the shards?
		return nil, fmt.Errorf("failed to heartbeat: %w", err)
	}

	// Handle migration mode logic
	switch migrationMode {
	case types.MigrationModeLOCALPASSTHROUGH:
		// LOCAL_PASSTHROUGH: statically assigned, stop heartbeating
		return nil, ErrLocalPassthroughMode

	case types.MigrationModeLOCALPASSTHROUGHSHADOW:
		// LOCAL_PASSTHROUGH_SHADOW: check response but don't apply it
		err = e.compareAssignments(shardAssignment)
		return nil, err

	case types.MigrationModeDISTRIBUTEDPASSTHROUGH:
		// DISTRIBUTED_PASSTHROUGH: validate then apply the assignment
		err = e.compareAssignments(shardAssignment)
		if err != nil {
			return nil, err
		}
		return shardAssignment, nil
		// Continue with applying the assignment from heartbeat

	case types.MigrationModeONBOARDED:
		// ONBOARDED: normal flow, apply the assignment from heartbeat
		return shardAssignment, nil
		// Continue with normal assignment logic below

	default:
		e.logger.Warn("unknown migration mode, skipping assignment",
			tag.ShardNamespace(e.namespace), tag.Dynamic("migration-mode", migrationMode))
		return nil, nil
	}
}

func (e *executorImpl[SP]) updateShardAssignmentMetered(ctx context.Context, shardAssignment map[string]*types.ShardAssignment) {
	startTime := e.timeSource.Now()
	defer e.metrics.
		Histogram(metricsconstants.ShardDistributorExecutorAssignLoopLatency, metricsconstants.ShardDistributorExecutorAssignLoopLatencyBuckets).
		RecordDuration(e.timeSource.Since(startTime))

	e.updateShardAssignment(ctx, shardAssignment)
}

func (e *executorImpl[SP]) heartbeat(ctx context.Context) (shardAssignments map[string]*types.ShardAssignment, migrationMode types.MigrationMode, err error) {
	return e.sendHeartbeat(ctx, types.ExecutorStatusACTIVE)
}

func (e *executorImpl[SP]) sendHeartbeat(ctx context.Context, status types.ExecutorStatus) (map[string]*types.ShardAssignment, types.MigrationMode, error) {
	// Fill in the shard status reports
	shardStatusReports := make(map[string]*types.ShardStatusReport)
	e.managedProcessors.Range(func(shardID string, managedProcessor *managedProcessor[SP]) bool {
		if managedProcessor.getState() == processorStateStarted {
			shardStatus := managedProcessor.processor.GetShardReport()

			shardStatusReports[shardID] = &types.ShardStatusReport{
				ShardLoad: shardStatus.ShardLoad,
				Status:    shardStatus.Status,
			}
		}
		return true
	})

	e.hostMetrics.Gauge(metricsconstants.ShardDistributorExecutorOwnedShards).Update(float64(len(shardStatusReports)))

	// Create the request
	request := &types.ExecutorHeartbeatRequest{
		Namespace:          e.namespace,
		ExecutorID:         e.executorID,
		Status:             status,
		ShardStatusReports: shardStatusReports,
		Metadata:           e.metadata.Get(),
	}

	// Send the request
	response, err := e.shardDistributorClient.Heartbeat(ctx, request)
	if err != nil {
		return nil, types.MigrationModeINVALID, fmt.Errorf("send heartbeat: %w", err)
	}

	previousMode := e.getMigrationMode()
	currentMode := response.MigrationMode
	if previousMode != currentMode {
		e.logger.Info("migration mode transition",
			tag.Dynamic("previous", previousMode),
			tag.Dynamic("current", currentMode),
			tag.ShardNamespace(e.namespace),
			tag.ShardExecutor(e.executorID))
		e.setMigrationMode(currentMode)
	}

	return response.ShardAssignments, response.MigrationMode, nil
}

func (e *executorImpl[SP]) sendDrainingHeartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), drainingHeartbeatTimeout)
	defer cancel()

	_, _, err := e.sendHeartbeat(ctx, types.ExecutorStatusDRAINING)
	if err != nil {
		e.logger.Error("failed to send draining heartbeat", tag.Error(err))
	}
}

func (e *executorImpl[SP]) updateShardAssignment(ctx context.Context, shardAssignments map[string]*types.ShardAssignment) {
	// Stop shard processing for shards not assigned to this executor.
	// Fired asynchronously so the heartbeat loop is not blocked waiting for Stop to complete.
	e.managedProcessors.Range(func(shardID string, managedProcessor *managedProcessor[SP]) bool {
		if assignment, ok := shardAssignments[shardID]; !ok || assignment.Status != types.AssignmentStatusREADY {
			e.stopManagerProcessor(shardID)
		}
		return true
	})

	// Start shard processing for shards assigned to this executor.
	// addManagerProcessor stores the processor immediately and fires Start asynchronously.
	for shardID, assignment := range shardAssignments {
		if assignment.Status == types.AssignmentStatusREADY {
			e.addManagerProcessor(ctx, shardID)
		}
	}
}

func (e *executorImpl[SP]) addNewShards(ctx context.Context, shardAssignments map[string]*types.ShardAssignment) {
	for shardID, assignment := range shardAssignments {
		if assignment.Status == types.AssignmentStatusREADY {
			e.addManagerProcessor(ctx, shardID)
		}
	}
}

func (e *executorImpl[SP]) deleteShards(shardIDs []string) {
	for _, shardID := range shardIDs {
		e.stopManagerProcessor(shardID)
	}
}

func (e *executorImpl[SP]) stopShardProcessors() {
	// Collect the done channels from every async stop goroutine so we can wait
	// for all of them to finish (respecting their internal 2-minute timeout) before
	// sending the draining heartbeat.
	var doneChs []<-chan struct{}

	e.managedProcessors.Range(func(shardID string, _ *managedProcessor[SP]) bool {
		if doneCh := e.stopManagerProcessor(shardID); doneCh != nil {
			doneChs = append(doneChs, doneCh)
		}
		return true
	})

	for _, doneCh := range doneChs {
		<-doneCh
	}
}

func (e *executorImpl[SP]) addManagerProcessor(ctx context.Context, shardID string) {
	if existing, ok := e.managedProcessors.Load(shardID); ok {
		// The processor already exists. If it is actively running or still starting
		// up, there is nothing to do. If it is stopping (async Stop() goroutine has
		// been fired but Delete() has not yet executed), we skip creating a new one
		// and log so the situation is visible. The next heartbeat will re-send this
		// shard as READY once the entry is gone, and addManagerProcessor will then
		// create a fresh processor.
		if existing.getState() == processorStateStopping {
			e.logger.Info("shard processor add skipped: existing processor is still stopping, will retry on next heartbeat",
				tag.Dynamic("shard-id", shardID))
		}
		return
	}

	e.metrics.Counter(metricsconstants.ShardDistributorExecutorShardsStarted).Inc(1)
	processor, err := e.shardProcessorFactory.NewShardProcessor(shardID)
	if err != nil {
		e.logger.Error("failed to create shard processor", tag.Error(err))
		e.metrics.Counter(metricsconstants.ShardDistributorExecutorProcessorCreationFailures).Inc(1)
		return
	}
	managedProcessor := newManagedProcessor(processor, processorStateStarting)
	e.managedProcessors.Store(shardID, managedProcessor)

	// Start the processor asynchronously with a timeout so the heartbeat loop
	// is not blocked. If the start exceeds the timeout, emit a metric and mark
	// the processor as stopping so it won't be reported as healthy.
	go func() {
		startCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), processorAsyncOperationTimeout)
		defer cancel()

		type startResult struct{ err error }
		resultCh := make(chan startResult, 1)
		go func() {
			resultCh <- startResult{err: processor.Start(startCtx)}
		}()

		select {
		case result := <-resultCh:
			if result.err != nil {
				e.logger.Error("shard processor start failed", tag.Dynamic("shard-id", shardID), tag.Error(result.err))
				managedProcessor.setState(processorStateStopping)
				return
			}
			managedProcessor.setState(processorStateStarted)
		case <-startCtx.Done():
			e.logger.Error("shard processor start timed out", tag.Dynamic("shard-id", shardID))
			e.metrics.Counter(metricsconstants.ShardDistributorExecutorProcessorStartTimeout).Inc(1)
			managedProcessor.setState(processorStateStopping)
		}
	}()
}

// stopManagerProcessor transitions the processor to stopping state and launches a
// goroutine to call Stop() and remove it from the map. It returns a channel that
// is closed once the goroutine finishes (including any timeout). Returns nil if
// the processor was not found or was already stopping.
func (e *executorImpl[SP]) stopManagerProcessor(shardID string) <-chan struct{} {
	managedProcessor, ok := e.managedProcessors.Load(shardID)
	// If the processor do not exist for the shard, or it is already stopping, skip it
	if !ok || managedProcessor.getState() == processorStateStopping {
		return nil
	}
	e.metrics.Counter(metricsconstants.ShardDistributorExecutorShardsStopped).Inc(1)
	managedProcessor.setState(processorStateStopping)

	doneCh := make(chan struct{})
	// Stop and remove the processor asynchronously with a timeout so the heartbeat
	// loop is not blocked. If the stop exceeds the timeout, emit a metric and force-
	// delete the processor from the map so it stops being tracked.
	go func() {
		defer close(doneCh)

		stopCtx, cancel := context.WithTimeout(context.Background(), processorAsyncOperationTimeout)
		defer cancel()

		stopResultCh := make(chan struct{})
		go func() {
			managedProcessor.processor.Stop()
			close(stopResultCh)
		}()

		select {
		case <-stopResultCh:
		case <-stopCtx.Done():
			e.logger.Error("shard processor stop timed out", tag.Dynamic("shard-id", shardID))
			e.metrics.Counter(metricsconstants.ShardDistributorExecutorProcessorStopTimeout).Inc(1)
		}
		e.managedProcessors.Delete(shardID)
	}()

	return doneCh
}

func (e *executorImpl[SP]) shardCleanUpLoop(ctx context.Context) {
	// We don't run the loop for invalid durations
	if e.ttlShard <= 0 {

		return
	}
	shardCleanUpTimer := e.timeSource.NewTimer(backoff.JitDuration(e.ttlShard, heartbeatJitterCoeff))
	defer shardCleanUpTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopC:
			return
		case <-shardCleanUpTimer.Chan():
			e.processorsToLastUse.Range(func(shardID string, time time.Time) bool {
				if time.Add(e.ttlShard).Before(e.timeSource.Now()) {
					if e.getMigrationMode() == types.MigrationModeONBOARDED {
						mp, ok := e.managedProcessors.Load(shardID)
						if ok {
							mp.processor.SetShardStatus(types.ShardStatusDONE)
						}
					} else {
						e.deleteShards([]string{shardID})
					}
					e.processorsToLastUse.Delete(shardID)
				}
				return true
			})
		}
	}
}

// compareAssignments compares the local assignments with the heartbeat response assignments
// return error if the assignment are not the same and emits convergence or divergence metrics
func (e *executorImpl[SP]) compareAssignments(heartbeatAssignments map[string]*types.ShardAssignment) error {
	// Get current local assignments
	localAssignments := make(map[string]bool)
	e.managedProcessors.Range(func(shardID string, managedProcessor *managedProcessor[SP]) bool {
		if managedProcessor.getState() == processorStateStarted {
			localAssignments[shardID] = true
		}
		return true
	})

	// Check if all local assignments are in heartbeat assignments with READY status
	for shardID := range localAssignments {
		assignment, exists := heartbeatAssignments[shardID]
		if !exists || assignment.Status != types.AssignmentStatusREADY {
			e.logger.Warn("assignment divergence: local shard not in heartbeat or not ready",
				tag.Dynamic("shard-id", shardID))
			e.emitMetricsConvergence(false)
			return ErrAssignmentDivergenceLocalShard
		}
	}

	// Check if all heartbeat READY assignments are in local assignments
	for shardID, assignment := range heartbeatAssignments {
		if assignment.Status == types.AssignmentStatusREADY {
			if !localAssignments[shardID] {
				e.logger.Warn("assignment divergence: heartbeat shard not in local",
					tag.Dynamic("shard-id", shardID))
				e.emitMetricsConvergence(false)
				return ErrAssignmentDivergenceHeartbeatShard
			}
		}
	}

	e.emitMetricsConvergence(true)
	return nil
}

func (e *executorImpl[SP]) emitMetricsConvergence(converged bool) {
	if converged {
		e.metrics.Counter(metricsconstants.ShardDistributorExecutorAssignmentConvergence).Inc(1)
	} else {
		e.metrics.Counter(metricsconstants.ShardDistributorExecutorAssignmentDivergence).Inc(1)
	}
}

func (e *executorImpl[SP]) GetNamespace() string {
	return e.namespace
}

func (e *executorImpl[SP]) SetMetadata(metadata map[string]string) {
	e.metadata.Set(metadata)
}

func (e *executorImpl[SP]) GetMetadata() map[string]string {
	return e.metadata.Get()
}
