package org.apache.flink.runtime.scheduler;

import com.alibaba.fastjson.JSON;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.newscheduler.Util;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class MyExecutionSlotAllocator implements ExecutionSlotAllocator {

	private static final Logger LOG = LoggerFactory.getLogger(MyExecutionSlotAllocator.class);

	private final SlotProvider slotProvider;

	public MyExecutionSlotAllocator(final SlotProvider slotProvider) {
		LOG.info("[HX] Using my executionSlotAllocator");
		this.slotProvider = slotProvider;
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
		}
		return null;
	}

	@Override
	public List<SlotExecutionVertexAssignment> allocateSlotsFor(List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
		List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			new ArrayList<>(executionVertexSchedulingRequirements.size());
		int maxParallelism = 1;
		for (ExecutionVertexSchedulingRequirements e : executionVertexSchedulingRequirements) {
			maxParallelism = Math.max(e.getExecutionVertexId().getSubtaskIndex() + 1, maxParallelism);
		}
		LOG.info("[HX] Job's max parallelism = {}", maxParallelism);

		List<Integer> placement = getPlacement();
		// TODO

		return null;
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
