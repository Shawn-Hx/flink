package org.apache.flink.runtime.scheduler;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.newscheduler.Util;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class MyExecutionSlotAllocator implements ExecutionSlotAllocator {

	private static final Logger LOG = LoggerFactory.getLogger(MyExecutionSlotAllocator.class);

	private final SlotProvider slotProvider;

	private final Time slotRequestTimeout;

	public MyExecutionSlotAllocator(final SlotProvider slotProvider, final Time slotRequestTimeout) {
		LOG.info("[HX] Using my executionSlotAllocator");
		this.slotProvider = slotProvider;
		this.slotRequestTimeout = slotRequestTimeout;
	}

	private List<Integer> getPlacement() {
		try {
			Process process = Runtime.getRuntime().exec(new String[]{Util.PYTHON, Util.SCRIPT_FILE});
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String placementStr = br.readLine();
			List<Integer> placement = JSON.parseArray(placementStr, Integer.class);
			LOG.info("[HX] placement: {}", placement);
			return placement;
		} catch (IOException e) {
			LOG.error("[HX] invoke python model error!");
			LOG.error(e.getMessage());
			return new ArrayList<>();
		}
	}

	@Override
	public List<SlotExecutionVertexAssignment> allocateSlotsFor(
		List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {

		int maxParallelism = 1;
		for (ExecutionVertexSchedulingRequirements e : executionVertexSchedulingRequirements) {
			maxParallelism = Math.max(e.getExecutionVertexId().getSubtaskIndex() + 1, maxParallelism);
		}
		LOG.info("[HX] Job's max parallelism = {}", maxParallelism);

		List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			new ArrayList<>(executionVertexSchedulingRequirements.size());

		// TODO
//		List<Integer> placement = getPlacement();
//		assert placement.size() == executionVertexSchedulingRequirements.size();
		List<Integer> placement = Arrays.asList(0, 1, 1, 0);	// [HX] for test
		for (int i = 0; i < executionVertexSchedulingRequirements.size(); i++) {
			final ExecutionVertexSchedulingRequirements schedulingRequirements =
				executionVertexSchedulingRequirements.get(i);
			final ExecutionVertexID executionVertexID = schedulingRequirements.getExecutionVertexId();
			final SlotRequestId slotRequestId = new SlotRequestId();
			final Integer resourceIndex = placement.get(i);
			CompletableFuture<LogicalSlot> slotFuture =
				slotProvider.allocateSlot(slotRequestId, resourceIndex, slotRequestTimeout);

			SlotExecutionVertexAssignment slotExecutionVertexAssignment =
				new SlotExecutionVertexAssignment(executionVertexID, slotFuture);

			slotFuture.whenComplete((ignored, throwable) -> {
				if (throwable != null) {
					slotProvider.cancelSlotRequest(slotRequestId, null, throwable);
				}
			});

			slotExecutionVertexAssignments.add(slotExecutionVertexAssignment);
		}

		return slotExecutionVertexAssignments;
	}

	@Override
	public void cancel(ExecutionVertexID executionVertexId) {
		LOG.error("[HX] ExecutionSlotAllocator.cancel() be invoked.");
	}

	@Override
	public CompletableFuture<Void> stop() {
		LOG.error("[HX] ExecutionSlotAllocator.stop() be invoked.");
		return null;
	}
}
