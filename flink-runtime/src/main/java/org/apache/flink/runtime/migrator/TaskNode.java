package org.apache.flink.runtime.migrator;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TaskNode {
    private final ExecutionVertex executionVertex;
    private final List<TaskNode> upstreamNodes = new ArrayList<>();
    private final List<TaskNode> downstreamNodes = new ArrayList<>();

    public TaskNode(ExecutionVertex executionVertex) {
        this.executionVertex = executionVertex;
    }

    public TaskManagerGateway getTaskManagerGateway() {
        return executionVertex.getCurrentExecutionAttempt().getAssignedResource().getTaskManagerGateway();
    }

    public Execution getExecution() {
        return executionVertex.getCurrentExecutionAttempt();
    }

    public ExecutionAttemptID getExecutionId(){
        return getExecution().getAttemptId();
    }

    public ExecutionVertexID getExecutionVertexID(){
        return executionVertex.getID();
    }

    public ExecutionAttemptID getCurrentExecutionID(){
        return executionVertex.getCurrentExecutionAttempt().getAttemptId();
    }

    public InputGateDeploymentDescriptor getIDD() {
        List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors = null;
        try {
            inputGateDeploymentDescriptors = TaskDeploymentDescriptorFactory
                    .fromExecutionVertex(
                            executionVertex,
                            executionVertex.getCurrentExecutionAttempt().getAttemptNumber())
                    .createInputGateDeploymentDescriptors();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inputGateDeploymentDescriptors.get(0);
    }

    public List<TaskNode> getUpstreamNodes() {
        return upstreamNodes;
    }

    public List<TaskNode> getDownstreamNodes() {
        return downstreamNodes;
    }

    public ExecutionVertex getExecutionVertex(){
        return executionVertex;
    }

}
