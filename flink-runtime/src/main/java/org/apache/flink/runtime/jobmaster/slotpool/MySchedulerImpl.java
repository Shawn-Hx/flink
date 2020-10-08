package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MySchedulerImpl implements Scheduler {

	private static final Logger log = LoggerFactory.getLogger(MySchedulerImpl.class);

	private static final int DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE = 16;

	/** The slot pool from which slots are allocated. */
	@Nonnull
	private final SlotPool slotPool;

	/** Executor for running tasks in the job master's main thread. */
	@Nonnull
	private ComponentMainThreadExecutor componentMainThreadExecutor;

	/** Managers for the different resources to place slots */
	private final Map<Integer, SlotSharingManager> slotSharingManagers;

	public MySchedulerImpl(
		@Nonnull SlotPool slotPool) {
		this.slotSharingManagers = new HashMap<>(DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE);
		this.slotPool = slotPool;
		this.componentMainThreadExecutor = new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
			"Scheduler is not initialized with proper main thread executor. " +
				"Call to Scheduler.start(...) required.");
	}

	@Override
	public void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor) {
		this.componentMainThreadExecutor = mainThreadExecutor;
	}

	@Override
	public boolean requiresPreviousExecutionGraphAllocations() {
		log.error("[HX] MySchedulerImpl.requiresPreviousExecutionGraphAllocations() be invoked.");
		return false;
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		log.error("[HX] MySchedulerImpl.returnLogicalSlot() be invoked.");
		SlotRequestId slotRequestId = logicalSlot.getSlotRequestId();
		SlotSharingGroupId slotSharingGroupId = logicalSlot.getSlotSharingGroupId();
		FlinkException cause = new FlinkException("Slot is being returned to the SlotPool.");
		cancelSlotRequest(slotRequestId, slotSharingGroupId, cause);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		Time allocationTimeout) {
		log.error("[HX] This method should not be invoked, there is must something wrong.");
		return null;
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
		SlotRequestId slotRequestId,
		Integer resourceId,
		Time allocationTimeout) {
		componentMainThreadExecutor.assertRunningInMainThread();

		final CompletableFuture<LogicalSlot> allocationResultFuture = new CompletableFuture<>();
		internalAllocateSlot(
			allocationResultFuture,
			slotRequestId,
			resourceId,
			allocationTimeout
		);
		return allocationResultFuture;
	}

	private void internalAllocateSlot(
		CompletableFuture<LogicalSlot> allocationResultFuture,
		SlotRequestId slotRequestId,
		Integer resourceId,
		Time allocationTimeout) {
		// TODO
	}

	@Override
	public void cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
		log.error("[HX] MySchedulerImpl.cancelSlotRequest() be invoked.");
		componentMainThreadExecutor.assertRunningInMainThread();

		if (slotSharingGroupId != null) {
			releaseSharedSlot(slotRequestId, slotSharingGroupId, cause);
		} else {
			slotPool.releaseSlot(slotRequestId, cause);
		}
	}

	private void releaseSharedSlot(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {
		log.error("[HX] MySchedulerImpl.releaseSharedSlot() be invoked.");
	}
}
