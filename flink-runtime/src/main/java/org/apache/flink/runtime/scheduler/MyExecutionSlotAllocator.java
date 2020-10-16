/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.newscheduler.Util;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * My ExecutionSlotAllocator which uses Python model to schedule.
 */
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
		// try to get placement from file first
		File placementFile = new File(Util.PLACEMENT_FILE);
		if (placementFile.exists()) {
			try (BufferedReader br = new BufferedReader(new FileReader(placementFile))) {
				String line = br.readLine();
				if (line != null) {
					LOG.info("[HX] get placement from file");
					return JSON.parseArray(line, Integer.class);
				}
			} catch (IOException e) {
				LOG.error("[HX] get placement from file error!");
				e.printStackTrace();
			}
		}
		// if file don't exist or the content is null, get placement from python model
		try {
			LOG.info("[HX] get placement from model");
			Process process = Runtime.getRuntime().exec(new String[]{Util.PYTHON, Util.SCRIPT_FILE});
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String placementStr = br.readLine();
			return JSON.parseArray(placementStr, Integer.class);
		} catch (IOException e) {
			LOG.error("[HX] invoke python model error!");
			e.printStackTrace();
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

		SlotExecutionVertexAssignment[] res = new SlotExecutionVertexAssignment[executionVertexSchedulingRequirements.size()];

		List<Integer> placement = getPlacement();
		LOG.info("[HX] placement: {}", placement);
		assert placement.size() == executionVertexSchedulingRequirements.size();
		Map<Integer, Set<ExecutionVertexSchedulingRequirements>> map = new TreeMap<>();
		for (int i = 0; i < placement.size(); i++) {
			map.computeIfAbsent(placement.get(i), k -> new HashSet<>())
					.add(executionVertexSchedulingRequirements.get(i));
		}
		int lastSlotIndex = -1;
		for (Map.Entry<Integer, Set<ExecutionVertexSchedulingRequirements>> entry: map.entrySet()) {
			int resourceIndex = entry.getKey();
			while (lastSlotIndex + 1 != resourceIndex) {
				slotProvider.allocateUselessSlot(slotRequestTimeout);
				lastSlotIndex++;
			}
			for (ExecutionVertexSchedulingRequirements schedulingRequirements : entry.getValue()) {
				final ExecutionVertexID executionVertexID = schedulingRequirements.getExecutionVertexId();
				final SlotRequestId slotRequestId = new SlotRequestId();
				CompletableFuture<LogicalSlot> slotFuture =
						slotProvider.allocateSlot(slotRequestId, resourceIndex, slotRequestTimeout);

				SlotExecutionVertexAssignment slotExecutionVertexAssignment =
						new SlotExecutionVertexAssignment(executionVertexID, slotFuture);

				slotFuture.whenComplete((ignored, throwable) -> {
					if (throwable != null) {
						slotProvider.cancelSlotRequest(slotRequestId, null, throwable);
					}
				});
				res[executionVertexSchedulingRequirements.indexOf(schedulingRequirements)] = slotExecutionVertexAssignment;
			}
			lastSlotIndex = resourceIndex;
		}

		return Arrays.asList(res);
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
