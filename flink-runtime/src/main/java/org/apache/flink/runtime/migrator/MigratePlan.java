package org.apache.flink.runtime.migrator;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class MigratePlan implements Serializable {
    final MigrateId migrateId;
    final JobID jobID;
    final ExecutionVertexID migratedTaskID;
    final List<ExecutionVertexID> upStreamTasksID;
    final List<ExecutionVertexID> downStreamTasksID;
    final ResourceID resourceID;

    public MigratePlan(
            MigrateId migrateId,
            JobID jobID,
            ExecutionVertexID migratedTaskID,
            List<ExecutionVertexID> upStreamTasksID,
            List<ExecutionVertexID> downStreamTasksID,
            ResourceID resourceID) {
        this.migrateId = migrateId;
        this.jobID = jobID;
        this.migratedTaskID = migratedTaskID;
        this.upStreamTasksID = upStreamTasksID;
        this.downStreamTasksID = downStreamTasksID;
        this.resourceID = resourceID;
    }

    public MigrateId getMigrateId() {
        return migrateId;
    }

    public JobID getJobID() {
        return jobID;
    }

    public ExecutionVertexID getMigratedTaskID() {
        return migratedTaskID;
    }

    public List<ExecutionVertexID> getUpStreamTasksID() {
        return upStreamTasksID;
    }

    public List<ExecutionVertexID> getDownStreamTasksID() {
        return downStreamTasksID;
    }

    // public String getMigratedTaskName() {
    //     return migratedTaskName;
    // }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigratePlan that = (MigratePlan) o;
        return Objects.equals(migrateId, that.migrateId) && Objects.equals(jobID, that.jobID)
                && Objects.equals(migratedTaskID, that.migratedTaskID) && Objects.equals(
                upStreamTasksID,
                that.upStreamTasksID) && Objects.equals(downStreamTasksID, that.downStreamTasksID)
                && Objects.equals(resourceID, that.resourceID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                migrateId,
                jobID,
                migratedTaskID,
                upStreamTasksID,
                downStreamTasksID,
                resourceID);
    }

    @Override
    public String toString() {
        return "MigratePlan{" +
                "migrateId=" + migrateId +
                ", jobID=" + jobID +
                ", migratedTaskID=" + migratedTaskID +
                ", upStreamTasksID=" + upStreamTasksID +
                ", downStreamTasksID=" + downStreamTasksID +
                ", resourceID=" + resourceID +
                '}';
    }
}
