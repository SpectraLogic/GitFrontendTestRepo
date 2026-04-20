package com.spectralogic.s3.dataplanner.backend.tape.api;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface TapeTaskQueue {

    void addStaticTask(StaticTapeTask task);

    void addDynamicTask(DynamicTapeTask task);

    List<IoTask> getTasksForChunk(final UUID chunkId);

    Set<IoTask> getTasksForJob(UUID jobId);

    boolean hasReadyTasks(final Predicate<TapeTask> predicate);

    /*
        NOTE: Though comparators we want to sort with will vary, they will be very similar, so simply resorting one list
        instead of maintaining several should be fast and memory-efficient
    */
    List<TapeTask> getSortedPendingTapeTasksFor(Comparator< TapeTask > comparator, Predicate<TapeTask> filter);

    List<TapeTask> getAllTapeTasks();

    Set<UUID> getChunkIds();

    List<TapeTask> get(UUID tapeId);


    /**
     * @param tapeId The tape id whose associated task will be updated.
     * @param tapeTaskType The type of the tape task.
     * @param newPriority The new priority for the specified tape's task to be updated to.
     * @param onlyChangePriorityIfIncreasingPriority If true, the priority can only be increased.
     *        If false, the priority can be both increased and decreased.
     * @param alwaysLogIfTaskExists Ensures a line is logged regardless of if the update occurs
     *        so long as the associated task exists within the queue.
     * @return A boolean indicating if the task exists within the queue.
     * This is used to determined if a new task needs to be created.
     */
    boolean tryPriorityUpdate(
            UUID tapeId,
            Class<?> tapeTaskType,
            BlobStoreTaskPriority newPriority,
            boolean onlyChangePriorityIfIncreasingPriority,
            boolean alwaysLogIfTaskExists);

    boolean remove(TapeTask tapeTask, String cause);

    int size();

    static String getTaskSummary(final List<TapeTask> list) {
        final Map<String, Long> tasksByType = list.stream()
                .collect(Collectors.groupingBy((TapeTask t) -> t.getClass().getSimpleName(), Collectors.counting()));
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, Long> taskByTypeEntry : tasksByType.entrySet()) {
            sb.append(" ").append(taskByTypeEntry.getKey()).append(" (").append(taskByTypeEntry.getValue()).append(")\n");
        }
        return sb.toString();
    }

    static String getTaskSummaryByState(final List<TapeTask> list) {
        final Map<BlobStoreTaskState, Map<String, Long>> tasksByType = list.stream().collect(
                Collectors.groupingBy(BlobStoreTask::getState,
                        Collectors.groupingBy((TapeTask t) -> t.getClass().getSimpleName(), Collectors.counting())));
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<BlobStoreTaskState, Map<String, Long>> taskEntriesByState : tasksByType.entrySet()) {
            sb.append(" ").append(taskEntriesByState.getKey()).append(":").append("\n");
            for (final Map.Entry<String, Long> taskByTypeEntry : taskEntriesByState.getValue().entrySet()) {
                sb.append("  ").append(taskByTypeEntry.getKey()).append(" (").append(taskByTypeEntry.getValue()).append(")\n");
            }
        }
        return sb.toString();
    }
}
