package org.apache.flink.runtime.migrator;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskMigrator {
    private static final Logger log =
            LoggerFactory.getLogger(TaskMigrator.class);

    public int endBarrierCnt;

    public Consumer<MigratePlan> func;

    public static Consumer<Long> reconnect;

    MigratePlan migratePlan;

    public TaskMigrator(MigratePlan  migratePlan,Consumer<MigratePlan> func ){
        this.migratePlan = migratePlan;
        this.func = func;
    }

    public MigratePlan getMigratePlan() {
        return migratePlan;
    }
}
