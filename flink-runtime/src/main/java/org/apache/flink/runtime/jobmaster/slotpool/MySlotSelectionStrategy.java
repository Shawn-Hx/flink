package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;

import javax.annotation.Nonnull;
import java.util.*;

public class MySlotSelectionStrategy extends LocationPreferenceSlotSelectionStrategy {

	@Nonnull
	@Override
	protected Optional<SlotInfoAndLocality> selectWithoutLocationPreference(@Nonnull Collection<SlotInfoAndResources> availableSlots, @Nonnull ResourceProfile resourceProfile) {
		List<SlotInfoAndResources> candidates = new ArrayList<>();
		for (SlotInfoAndResources candidate : availableSlots) {
			if (candidate.getRemainingResources().isMatching(resourceProfile)) {
				candidates.add(candidate);
			}
		}
		if (!candidates.isEmpty()) {
			SlotInfoAndResources last = candidates.get(candidates.size() - 1);
			return Optional.of(SlotInfoAndLocality.of(last.getSlotInfo(), Locality.UNCONSTRAINED));
		}
		return Optional.empty();
	}

	@Override
	protected double calculateCandidateScore(int localWeigh, int hostLocalWeigh, double taskExecutorUtilization) {
		return localWeigh * 2 + hostLocalWeigh;
	}
}
