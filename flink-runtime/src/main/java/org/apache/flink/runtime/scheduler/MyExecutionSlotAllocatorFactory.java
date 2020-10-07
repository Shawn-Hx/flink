package org.apache.flink.runtime.scheduler;


import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

public class MyExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {

	private final SlotProvider slotProvider;

	public MyExecutionSlotAllocatorFactory(final SlotProvider slotProvider) {
		this.slotProvider = slotProvider;
	}

	@Override
	public ExecutionSlotAllocator createInstance(InputsLocationsRetriever inputsLocationsRetriever) {
		return new MyExecutionSlotAllocator(slotProvider);
	}

}
