package org.apache.flink.runtime.migrator;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.Objects;

public class EndBarrierMarker extends RuntimeEvent {
    long migrateId;
    transient int location = -1;

    public EndBarrierMarker(long migrateId) {
        this.migrateId = migrateId;
    }

    public EndBarrierMarker(long migrateId, int location) {
        this.migrateId = migrateId;
        this.location = location;
    }

    public long getMigrateId() {
        return migrateId;
    }

    public int getLocation() {
        return location;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException(
                "EndBarrierMarker this method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException(
                "EndBarrierMarker this method should never be called");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EndBarrierMarker that = (EndBarrierMarker) o;
        return migrateId == that.migrateId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(migrateId);
    }

    @Override
    public String toString() {
        return "EndBarrierMarker{" +
                "migrateId=" + migrateId +
                '}';
    }

}
