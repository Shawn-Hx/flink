package org.apache.flink.runtime.migrator;

import org.apache.flink.util.AbstractID;

public class MigrateId  extends AbstractID {
    @Override
    public String toString() {
        return "MigrateId{" + super.toString() + '}';
    }
}
