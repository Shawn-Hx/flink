package org.apache.flink.runtime.migrator;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.ExecutionGraphToInputsLocationsRetrieverAdapter;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Graph {
    private final Map<ExecutionVertexID, TaskNode> taskNodes = new LinkedHashMap<>();
    private final ExecutionGraph executionGraph;

    public Graph(
            ExecutionGraph executionGraph) {
        this.executionGraph = executionGraph;
    }

    public void generateGraph() {
        ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever
                = new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);
        for (SchedulingExecutionVertex executionVertex : executionGraph
                .getSchedulingTopology()
                .getVertices()) {
            Execution execution = getExecution(executionVertex.getId());
            if (execution.getState() != ExecutionState.RUNNING) {
                taskNodes.clear();
                return;
            }
            TaskNode taskNode = new TaskNode(execution.getVertex());
            taskNodes.put(execution.getVertex().getID(), taskNode);
            // add upstream and downstream vertices info
            for (Collection<ExecutionVertexID> producers :
                    inputsLocationsRetriever.getConsumedResultPartitionsProducers(executionVertex.getId())) {
                for (ExecutionVertexID producer : producers) {
                    TaskNode upStreamNode = taskNodes.get(getExecution(producer)
                            .getVertex()
                            .getID());
                    upStreamNode.getDownstreamNodes().add(taskNode);
                    taskNode.getUpstreamNodes().add(upStreamNode);
                }
            }
        }
    }


    private Execution getExecution(ExecutionVertexID executionVertexID) {
        return executionGraph
                .getAllVertices()
                .get(executionVertexID.getJobVertexId())
                .getTaskVertices()[executionVertexID.getSubtaskIndex()]
                .getCurrentExecutionAttempt();
    }

    public ExecutionGraph getExecutionGraph() {
        return executionGraph;
    }

    public TaskNode getTaskNode(ExecutionVertexID vertexID) {
        return taskNodes.get(vertexID);
    }

    public Map<ExecutionVertexID, TaskNode> getTaskNodes() {
        return taskNodes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Map<ExecutionVertexID, Integer> idMap = new HashMap<>();
        taskNodes.keySet().forEach(node -> idMap.put(node, idMap.size()));
        for (Map.Entry<ExecutionVertexID, TaskNode> entry : taskNodes.entrySet()) {
            ExecutionVertexID vertexID = entry.getKey();
            TaskNode taskNode = entry.getValue();
            String id = idMap.get(vertexID).toString();
            String taskNameWithSubtaskIndex = taskNode
                    .getExecutionVertex()
                    .getTaskNameWithSubtaskIndex();
            String upStreamNodes = taskNode
                    .getUpstreamNodes()
                    .stream()
                    .map(x -> idMap.get(x.getExecutionVertexID()).toString())
                    .collect(Collectors.joining("-"));
            String downStreamNodes = taskNode
                    .getDownstreamNodes()
                    .stream()
                    .map(x -> idMap.get(x.getExecutionVertexID()).toString())
                    .collect(Collectors.joining("-"));
            String[] node = {id, taskNameWithSubtaskIndex, upStreamNodes, downStreamNodes};
            sb.append(String.join(",", node));
            sb.append('\n');
        }
        return sb.toString();
    }
}
