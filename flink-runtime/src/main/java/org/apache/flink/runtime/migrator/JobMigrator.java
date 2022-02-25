package org.apache.flink.runtime.migrator;


import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobMigrator implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(JobMigrator.class);

    private final Map<Long, TaskStateSnapshot> checkpoints = new HashMap<>();
    Graph graph;
    ScheduledExecutor mainThreadExecutor;
    MigrateScheduler migrateScheduler;
    Map<MigrateId, Migrate> migrates = new HashMap<>();
    Deque<MigratePlan> migratePlanQueue = new ArrayDeque<>();
    List<Execution> oldExecutions = new ArrayList<>();

    DefaultScheduler defaultScheduler;

    public JobMigrator(
            DefaultScheduler defaultScheduler,
            ScheduledExecutor mainThreadExecutor) {
        this.mainThreadExecutor = mainThreadExecutor;
        this.graph = new Graph(defaultScheduler.getExecutionGraph());
        this.defaultScheduler = defaultScheduler;
        this.migrateScheduler = new MigrateScheduler(graph);
        mainThreadExecutor.schedule(this, 0L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        if (graph.getTaskNodes().size() == 0) {
            graph.generateGraph();
        }else{
            try {
                migrateScheduler.writeInfo();
                checkNewPlacement();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        mainThreadExecutor.schedule(this, 100, TimeUnit.MILLISECONDS);
    }

    void checkNewPlacement() throws Exception {
        Path placementPath = Path.of("/root/flink/placements-new");
        if (Files.exists(placementPath)) {
            String newPlacements = Files.readString(placementPath);
            Files.delete(placementPath);
            List<MigratePlan> migratePlans = migrateScheduler.generateMigratePlans(newPlacements);
            migratePlanQueue.addAll(migratePlans);
            log.info("[JY] placement from {} to {}, migrate plans are {}",
                    migrateScheduler.getCurrentPlacements(),
                    newPlacements,
                    migratePlans);
            startMigrate(null);
        }
    }

    public void startMigrate(MigratePlan migratePlan) {
        if (migratePlan == null) {
            migratePlan = migratePlanQueue.pollFirst();
            if (migratePlan == null) {
                if (oldExecutions.size() > 0) {
                    for (Execution oldExecution : oldExecutions) {
                        log.info("[JY] oldExecution.fail");
                        oldExecution.fail(new Exception("migrated"));
                    }
                    oldExecutions.clear();
                    migrateScheduler.releaseResource();
                }
                checkpoints.clear();
                return;
            }
        }
        migrates.put(migratePlan.getMigrateId(), new Migrate(migratePlan));

        // 准备接受checkpoint
        checkpoints.put(migratePlan.getMigrateId().getLowerPart(), null);
        log.info(
                "[JY] doMigrate={}, migratePlan={}",
                graph
                        .getTaskNode(migratePlan.getMigratedTaskID())
                        .getExecutionVertex()
                        .getTaskNameWithSubtaskIndex(),
                migratePlan);
        notifyState(migratePlan);
    }

    // 迁移节点发送预通知，上游节点发送迁移通知
    private void notifyState(MigratePlan migratePlan) {
        log.info("[JY] notifyState{}", migratePlan);
        try {
            TaskNode taskNode = graph.getTaskNode(migratePlan.getMigratedTaskID());
            log.info("[JY] send migratePlan to migrated node");
            CompletableFuture<Acknowledge> acknowledgeCompletableFuture = taskNode.
                    getTaskManagerGateway().
                    sendMigratePlan(migratePlan, taskNode.getExecution().getAttemptId(), -1);
            acknowledgeCompletableFuture.get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (ExecutionVertexID executionVertexID : migratePlan.getUpStreamTasksID()) {
            try {
                TaskNode upStreamTaskNode = graph.getTaskNode(executionVertexID);
                TaskNode migrateTaskNode = graph.getTaskNode(migratePlan.getMigratedTaskID());
                InputGateDeploymentDescriptor idd = migrateTaskNode.getIDD();
                int consumedSubpartitionIndex = idd.getConsumedSubpartitionIndex();

                log.info(
                        "[JY] send migratePlan to upSteam node, location ={}",
                        consumedSubpartitionIndex);

                upStreamTaskNode
                        .getTaskManagerGateway()
                        .sendMigratePlan(
                                migratePlan,
                                upStreamTaskNode.getExecutionId(),
                                consumedSubpartitionIndex);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (migratePlan.getDownStreamTasksID().size() == 0) {
            log.info("[JY] downstream size = 0, create task immediately");
            createTask(migratePlan);
        }
    }

    // 新节点reportNewNodeOK
    public void reportNewNodeOK(long migrateId) {
        log.info("[JY] reportNewNodeOK");
        log.info("[JY] migrateId={}", migrateId);
        for (MigrateId id : migrates.keySet()) {
            if (id.getLowerPart() == migrateId) {
                Migrate migratePlan = migrates.get(id);
                log.info("[JY] migratePlan.down={}", migratePlan.down);
                log.info("[JY] migratePlan.down++");
                migratePlan.down++;
                if (migratePlan.down == migratePlan.migratePlan.getDownStreamTasksID().size()) {
                    log.info("[JY] start createTask");
                    createTask(migratePlan.migratePlan);
                }
            }
        }
    }

    public boolean reportCheckpoint(long checkpointId, TaskStateSnapshot taskStateSnapshot) {
        if (checkpoints.containsKey(checkpointId)) {
            checkpoints.put(checkpointId, taskStateSnapshot);
            return true;
        } else {
            return false;
        }
    }

    // 创建新节点
    private void createTask(MigratePlan migratePlan) {
        Migrate migrate = migrates.get(migratePlan.getMigrateId());
        ExecutionVertexID executionVertexID = migratePlan.getMigratedTaskID();
        long checkpointId = migrate.migratePlan.getMigrateId().getLowerPart();
        JobManagerTaskRestore jobManagerTaskRestore;
        synchronized (checkpoints) {
            log.info("[JY] checkpointId={},checkpoints={}", checkpointId, checkpoints);
            if (checkpoints.get(checkpointId) != null) {
                TaskStateSnapshot taskStateSnapshot = checkpoints.get(checkpointId);
                jobManagerTaskRestore = new JobManagerTaskRestore(checkpointId, taskStateSnapshot);
                log.info("[JY] find state");
            } else {
                mainThreadExecutor.schedule(() -> {
                    createTask(migratePlan);
                }, 50, TimeUnit.MILLISECONDS);
                return;
            }
        }
        log.info("[JY] createTask");
        Execution oldExecution = defaultScheduler.migrateTask(
                executionVertexID,
                jobManagerTaskRestore,
                migratePlan.resourceID,
                migrateScheduler::allocateSharedSlot);
        oldExecutions.add(oldExecution);

        Execution newExecution = graph
                .getTaskNode(migrate.migratePlan.getMigratedTaskID())
                .getExecution()
                .getVertex()
                .getCurrentExecutionAttempt();
        log.info("[JY] start downStreamReconnect");
        downStreamReconnect(migratePlan, newExecution);
    }

    // 当新任务部署完成后，下游重建连接
    void downStreamReconnect(MigratePlan migratePlan, Execution newExecution) {
        if (newExecution.getState() != ExecutionState.RUNNING) {
            mainThreadExecutor.schedule(() -> {
                try {
                    downStreamReconnect(migratePlan, newExecution);
                } catch (Exception e) {
                    e.printStackTrace();
                    log.info("[JY] error");
                }
            }, 50, TimeUnit.MILLISECONDS);
            return;
        }

        log.info("[JY] downStreamReconnect,{}", migratePlan);
        ExecutionAttemptID migratedID = newExecution.getAttemptId();

        for (ExecutionVertexID executionVertexID : migratePlan.getDownStreamTasksID()) {
            log.info("[JY] reconnect {}", executionVertexID);
            TaskNode downSteamNode = graph.getTaskNode(executionVertexID);
            if (downSteamNode == null) {
                log.info("[JY] downSteamNode == null");
            }

            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors = null;
            try {
                inputGateDeploymentDescriptors = TaskDeploymentDescriptorFactory
                        .fromExecutionVertex(
                                downSteamNode.getExecutionVertex(),
                                downSteamNode
                                        .getExecutionVertex()
                                        .getCurrentExecutionAttempt()
                                        .getAttemptNumber())
                        .createInputGateDeploymentDescriptors();
            } catch (IOException e) {
                e.printStackTrace();
            }

            InputGateDeploymentDescriptor idd = inputGateDeploymentDescriptors.get(0);
            ShuffleDescriptor[] shuffleDescriptors = idd.getShuffleDescriptors();
            int index = 0;
            for (int i = 0; i < shuffleDescriptors.length; i++) {
                if (shuffleDescriptors[i]
                        .getResultPartitionID()
                        .getProducerId()
                        .equals(migratedID)) {
                    index = i;
                    log.info("[JY] find shuffleDescriptor equal");
                } else {
                    log.info("[JY] not find shuffleDescriptor equal");
                }
            }
            int consumedSubpartitionIndex = idd.getConsumedSubpartitionIndex();
            TaskManagerGateway taskManagerGateway = downSteamNode.getTaskManagerGateway();
            try {
                taskManagerGateway.connectUpstreamNodes(
                        downSteamNode.getExecutionId(),
                        new PartitionInfo(idd.getConsumedResultId(), shuffleDescriptors[index]),
                        idd,
                        index,
                        consumedSubpartitionIndex);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        restoreUpstream(migratePlan);
    }

    void restoreUpstream(MigratePlan migratePlan) {
        log.info("[JY] restoreUpstream");
        for (ExecutionVertexID executionVertexID : migratePlan.getUpStreamTasksID()) {
            try {
                TaskNode taskNode = graph.getTaskNode(executionVertexID);
                taskNode
                        .getTaskManagerGateway()
                        .sendMigratePlan(migratePlan, taskNode.getExecutionId(), 0);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        releaseResource(migratePlan);
    }

    // 回收资源,恢复事件
    void releaseResource(MigratePlan migratePlan) {
        log.info("[JY] releaseResource,{}", migratePlan);
        startMigrate(null);
    }
}
