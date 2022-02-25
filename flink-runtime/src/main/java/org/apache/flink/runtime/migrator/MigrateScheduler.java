package org.apache.flink.runtime.migrator;


import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotTracker;
import org.apache.flink.runtime.scheduler.SharedSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MigrateScheduler {
    private static final Logger log = LoggerFactory.getLogger(MigrateScheduler.class);
    public static DefaultSlotTracker slotTracker;
    public static DefaultDeclarativeSlotPool slotPool;

    public static List<SharedSlot> resources = new ArrayList<>();

    Graph graph;

    MigrateScheduler(Graph graph) {
        this.graph = graph;
    }

    public static void registerSharedSlots(Collection<SharedSlot> slots) {
        resources.addAll(slots);
    }

    void writeInfo() throws Exception {
        // write resources
        List<String> resources = getResources();
        Files.writeString(Path.of("/root/flink/resources"), String.join("\n", resources));
        // write placements
        int[] currentLocations = getCurrentPlacements();
        Files.writeString(Path.of("/root/flink/placements"), Arrays.toString(currentLocations));
        // write graph
        String graphInfo = graph.toString();
        Files.writeString(Path.of("/root/flink/graph"), graphInfo);
    }

    List<MigratePlan> generateMigratePlans(String placement) throws Exception {
        placement = placement.split("\n")[0];
        String[] placements = placement.substring(1, placement.length() - 1).split(",");
        int[] newPlacements = new int[placements.length];
        for (int i = 0; i < placements.length; i++) {
            newPlacements[i] = Integer.parseInt(placements[i]);
        }
        List<MigratePlan> migratePlans = new ArrayList<>();
        int[] oldPlacements = getCurrentPlacements();
        if (oldPlacements.length != newPlacements.length) {
            throw new Exception(
                    "oldPlacements +" + Arrays.toString(newPlacements) + " length error");
        }
        List<ResourceID> resources = slotTracker.getAllSlots().stream()
                .map(x -> x.getTaskManagerConnection().getResourceID())
                .sorted(Comparator.comparing(ResourceID::getResourceIdString))
                .collect(Collectors.toList());
        List<TaskNode> taskNodes = new ArrayList<>(graph.getTaskNodes().values());
        for (int i = 0; i < oldPlacements.length; i++) {
            if (newPlacements[i] > resources.size()) {
                throw new Exception(
                        "oldPlacements +" + Arrays.toString(newPlacements) + " location error");
            }
            if (oldPlacements[i] != newPlacements[i]) {
                TaskNode taskNode = taskNodes.get(i);
                ResourceID resourceID = resources.get(newPlacements[i]);
                List<ExecutionVertexID> upStreamTasksID = taskNode.getUpstreamNodes().stream()
                        .map(TaskNode::getExecutionVertexID).collect(Collectors.toList());
                List<ExecutionVertexID> downStreamTasksID = taskNode.getDownstreamNodes().stream()
                        .map(TaskNode::getExecutionVertexID).collect(Collectors.toList());
                migratePlans.add(new MigratePlan(
                        new MigrateId(),
                        graph.getExecutionGraph().getJobID(),
                        taskNode.getExecutionVertexID(),
                        upStreamTasksID,
                        downStreamTasksID,
                        resourceID));
            }
        }
        return migratePlans;
    }

    public SharedSlot getSharedSlot(ResourceID resourceID) {
        for (SharedSlot sharedSlot : resources) {
            try {
                if (sharedSlot.getSlotContextFuture().get().getTaskManagerLocation().getResourceID()
                        .equals(resourceID)) {
                    return sharedSlot;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public SharedSlot getSharedSlot(ExecutionVertexID vertexID) {
        for (SharedSlot sharedSlot : resources) {
            try {
                if (sharedSlot.getExecutionSlotSharingGroup().executionVertexIds.contains(vertexID)) {
                    return sharedSlot;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public SharedSlot allocateSharedSlot(
            ExecutionVertexID vertexID,
            ResourceID newResourceID) {
        // release old resource
        SharedSlot oldSharedSlot = getSharedSlot(vertexID);
        oldSharedSlot.getExecutionSlotSharingGroup().removeVertex(vertexID);
        // try allocate
        SharedSlot sharedSlot = getSharedSlot(newResourceID);
        if (sharedSlot != null) {
            log.info("[JY] resource reuse");
            sharedSlot.getExecutionSlotSharingGroup().addVertex(vertexID);
        } else{
            log.info("[JY] resource new");
            slotTracker.moveToFirstFree(newResourceID);
        }
        return sharedSlot;
    }

    public void releaseResource(){
        List<SharedSlot> removedSharedSlot = new ArrayList<>();
        for (SharedSlot sharedSlot : resources) {
            if (sharedSlot.getExecutionSlotSharingGroup().executionVertexIds.size() == 0) {
                log.info("[JY] find empty shared slot:{}",sharedSlot);
                removedSharedSlot.add(sharedSlot);

            }
        }
        for (SharedSlot sharedSlot : removedSharedSlot) {
            resources.remove(sharedSlot);
            sharedSlot.release(new Exception("remove shared slot"));
        }

        slotPool.releaseIdleSlots(Long.MAX_VALUE);
    }

    List<String> getResources() {
        return slotTracker.getAllSlots().stream()
                .map(x -> x.getTaskManagerConnection().getResourceID().getResourceIdString())
                .sorted()
                .collect(Collectors.toList());
    }

    int[] getCurrentPlacements() throws Exception {
        List<String> collect = getResources();
        Map<String, Integer> resourceIDIntegerMap = new HashMap<>();
        int cnt = 0;
        for (String id : collect) {
            resourceIDIntegerMap.put(id, cnt++);
        }
        int[] locations = new int[graph.getTaskNodes().size()];
        int id = 0;
        for (Map.Entry<ExecutionVertexID, TaskNode> entry : graph
                .getTaskNodes()
                .entrySet()) {
            TaskNode node = entry.getValue();
            ResourceID resourceID = node
                    .getExecution()
                    .getTaskManagerLocationFuture()
                    .get()
                    .getResourceID();
            int location = resourceIDIntegerMap.get(resourceID.getResourceIdString());
            locations[id++] = location;
        }
        return locations;
    }
}
