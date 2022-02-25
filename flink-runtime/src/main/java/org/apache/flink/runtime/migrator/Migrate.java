package org.apache.flink.runtime.migrator;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.HashMap;
import java.util.Map;

public class Migrate {
    public enum State{
        INIT,
        CREATED,
        DEPLOYED,
    }

    public MigratePlan migratePlan;

    public int down;

    State state = State.INIT;

    // public Map<ExecutionVertexID, InputGateDeploymentDescriptor> oldIDD = new HashMap<>();

    Migrate(MigratePlan migratePlan){
        this.migratePlan = migratePlan;
    }

}
