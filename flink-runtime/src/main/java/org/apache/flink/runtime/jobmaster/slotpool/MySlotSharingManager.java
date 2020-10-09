package org.apache.flink.runtime.jobmaster.slotpool;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.*;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

public class MySlotSharingManager {

	private static final Logger LOG = LoggerFactory.getLogger(SlotSharingManager.class);

	private final Integer resourceIndex;

	private final AllocatedSlotActions allocatedSlotActions;

	private final SlotOwner slotOwner;

	private final Map<SlotRequestId, TaskSlot> allTaskSlots;

	@Nullable
	private MultiTaskSlot root;

	private boolean isResolved;

	@Nullable
	private TaskManagerLocation taskManagerLocation;
	@Nullable
	private AllocationID allocationID;

	MySlotSharingManager(
			Integer resourceIndex,
			AllocatedSlotActions allocatedSlotActions,
			SlotOwner slotOwner) {
		this.resourceIndex = Preconditions.checkNotNull(resourceIndex);
		this.allocatedSlotActions = Preconditions.checkNotNull(allocatedSlotActions);
		this.slotOwner = Preconditions.checkNotNull(slotOwner);

		allTaskSlots = new HashMap<>(16);
	}

	@Nullable
	TaskSlot getTaskSlot(SlotRequestId slotRequestId) {
		return allTaskSlots.get(slotRequestId);
	}

	@Nonnull
	MultiTaskSlot createRootSlot(
		SlotRequestId slotRequestId,
		CompletableFuture<? extends SlotContext> slotContextFuture,
		SlotRequestId allocatedSlotRequestId) {
		LOG.debug("Create multi task slot [{}] in slot [{}].", slotRequestId, allocatedSlotRequestId);

		final CompletableFuture<SlotContext> slotContextFutureAfterRootSlotResolution = new CompletableFuture<>();
		final MultiTaskSlot rootMultiTaskSlot = createAndRegisterRootSlot(
			slotRequestId,
			allocatedSlotRequestId,
			slotContextFutureAfterRootSlotResolution);

		FutureUtils.forward(
			slotContextFuture.thenApply(
				(SlotContext slotContext) -> {
					tryMarkSlotAsResolved(slotRequestId, slotContext);
					return slotContext;
				}),
			slotContextFutureAfterRootSlotResolution);

		return rootMultiTaskSlot;
	}

	private MultiTaskSlot createAndRegisterRootSlot(
		SlotRequestId slotRequestId,
		SlotRequestId allocatedSlotRequestId,
		CompletableFuture<? extends SlotContext> slotContextFuture) {

		final MultiTaskSlot rootMultiTaskSlot = new MultiTaskSlot(
			slotRequestId,
			slotContextFuture,
			allocatedSlotRequestId);

		allTaskSlots.put(slotRequestId, rootMultiTaskSlot);
		this.root = rootMultiTaskSlot;
		return rootMultiTaskSlot;
	}

	private void tryMarkSlotAsResolved(SlotRequestId slotRequestId, SlotInfo slotInfo) {
		if (!isResolved) {
			final AllocationID allocationId = slotInfo.getAllocationId();
			LOG.trace("Fulfill multi task slot [{}] with slot [{}].", slotRequestId, allocationId);
			this.isResolved = true;
			this.taskManagerLocation = slotInfo.getTaskManagerLocation();
			this.allocationID = allocationId;
		}
	}

	public MultiTaskSlot getRootMultiTaskSlot() {
		if (this.root == null) {
			LOG.error("[HX] root be got before being created");
		}
		return this.root;
	}

	/**
	 * Base class for all task slots.
	 */
	public abstract static class TaskSlot {
		// every TaskSlot has an associated slot request id
		private final SlotRequestId slotRequestId;

		TaskSlot(SlotRequestId slotRequestId) {
			this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
		}

		public SlotRequestId getSlotRequestId() {
			return slotRequestId;
		}

		public abstract void release(Throwable cause);
	}

	public final class MultiTaskSlot extends TaskSlot implements PhysicalSlot.Payload {

		private	final Set<TaskSlot> children;

		// [HX] We don't need this field, because only roots are MultiTaskSlots
//		@Nullable
//		private final MultiTaskSlot parent;

		private final CompletableFuture<? extends SlotContext> slotContextFuture;

		@Nullable
		private final SlotRequestId allocatedSlotRequestId;

		private MultiTaskSlot(
			SlotRequestId slotRequestId,
			CompletableFuture<? extends SlotContext> slotContextFuture,
			@Nullable SlotRequestId allocatedSlotRequestId) {
			super(slotRequestId);
			Preconditions.checkNotNull(slotContextFuture);

			this.allocatedSlotRequestId = allocatedSlotRequestId;

			this.children = new HashSet<>();

			this.slotContextFuture = slotContextFuture.handle((SlotContext slotContext, Throwable throwable) -> {
				if (throwable != null) {
					LOG.error("[HX] something wrong with slotContextFuture");
					// If the underlying resource request failed, we currently fail all the requests
					release(throwable);
					throw new CompletionException(throwable);
				}
				// [HX] don't care about resource profile
//				if (parent == null) {
//					// sanity check
//					releaseSlotIfOversubscribing(slotContext);
//				}
				return slotContext;
			});
		}

		CompletableFuture<? extends SlotContext> getSlotContextFuture() {
			return slotContextFuture;
		}

		SingleTaskSlot allocateSingleTaskSlot(SlotRequestId slotRequestId) {
			final SingleTaskSlot leaf = new SingleTaskSlot(slotRequestId, this);
			children.add(leaf);

			allTaskSlots.put(slotRequestId, leaf);
			return leaf;
		}

		@Override
		public void release(Throwable cause) {
			LOG.info("[HX] MultiTaskSlot.release() be invoked.");
		}
	}

	public final class SingleTaskSlot extends TaskSlot {

		private final MultiTaskSlot parent;

		private final CompletableFuture<SingleLogicalSlot> singleLogicalSlotFuture;

		private SingleTaskSlot(
			SlotRequestId slotRequestId,
			MultiTaskSlot parent) {
			super(slotRequestId);

			this.parent = Preconditions.checkNotNull(parent);
			singleLogicalSlotFuture = parent.getSlotContextFuture()
				.thenApply(
					(SlotContext slotContext) -> {
						return new SingleLogicalSlot(
							slotRequestId,
							slotContext,
							null,	// TODO is null ok?
							Locality.UNCONSTRAINED,	// TODO is UNCONSTRAINED ok?
							slotOwner);
					});
		}

		CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
			return singleLogicalSlotFuture.thenApply(Function.identity());
		}

		@Override
		public void release(Throwable cause) {
			LOG.error("[HX] SingleTaskSlot.release() be invoked.");
		}
	}

}
