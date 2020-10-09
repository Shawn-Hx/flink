package org.apache.flink.runtime.scheduler;


import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

public class MyExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {

	private final SlotProvider slotProvider;
	private final Time slotRequestTimeout;

	public MyExecutionSlotAllocatorFactory(final SlotProvider slotProvider, final Time slotRequestTimeout) {
		this.slotProvider = slotProvider;
		this.slotRequestTimeout = slotRequestTimeout;
	}

	@Override
	public ExecutionSlotAllocator createInstance(InputsLocationsRetriever inputsLocationsRetriever) {
		return new MyExecutionSlotAllocator(slotProvider, slotRequestTimeout);
	}

}
