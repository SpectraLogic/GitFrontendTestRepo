package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.dataplanner.backend.tape.api.*;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;


public class TapeTaskQueueImpl implements TapeTaskQueue {

    public TapeTaskQueueImpl(final TapeTaskDequeuedListener listener, final Object taskStateLock) {
        m_tapeTaskDequeuedListener = listener;
        m_taskStateLock = taskStateLock;
    }


    @Override
    public void addStaticTask(final StaticTapeTask task) {
        synchronized ( m_taskStateLock ) {
            final Collection<StaticTapeTask> existingTasks = m_staticTasksByTapeId.get(task.getTapeId());
            if (existingTasks != null && !task.allowMultiplePerTape()) {
                final Optional<StaticTapeTask> existingTask = existingTasks.stream().filter((t) -> {
                    return t.getClass() == task.getClass();
                }).findFirst();
                existingTask.ifPresent((t) -> {
                    throw new DataPlannerException(GenericFailure.CONFLICT, "Cannot schedule another \"" + task.getClass().getSimpleName() + "\" task for tape " + task.getTapeId() + " because one is already scheduled.");
                });
            }
            m_staticTasksByTapeId.put(task.getTapeId(), task);
            addTaskInternal(task);
        }
    }

    @Override
    public void addDynamicTask(final DynamicTapeTask task) {
        synchronized ( m_taskStateLock ) {
            addTaskInternal(task);
        }
    }

    private void addTaskInternal(final TapeTask task) {
        m_sortableTaskList.add(task);
        if (task instanceof IoTask) {
            final IoTask ioTask = (IoTask)task;
            final Collection<UUID> chunkIds = ioTask.getChunkIds();
            final UUID[] jobIds = ioTask.getJobIds();
            for (final UUID chunkId : chunkIds) {
                m_ioTasksByChunkId.put(chunkId, ioTask);
            }
            for (final UUID jobId : jobIds) {
                m_ioTasksByJobId.put(jobId, ioTask);
            }
            LOG.info( "Enqueued " + task.toString() + " servicing " + chunkIds.size() + " chunks at priority " + task.getPriority() + "." );
        } else {
            LOG.info( "Enqueued " + task.toString() + " at priority " + task.getPriority() + "." );
        }
    }

    private boolean removeTask(final StaticTapeTask task, final String cause) {
        validateDequeue(task);
        if (m_staticTasksByTapeId.containsKey(task.getTapeId())) {
            m_staticTasksByTapeId.get(task.getTapeId()).remove(task);
        } else {
            LOG.warn("Removing task " + task.toString() + " for tape " + task.getTapeId() + " but no such task was found for that tape ID.");
        }
        return removeTaskInternal(task, cause);
    }

    private boolean removeTask(final DynamicTapeTask task, final String cause) {
        validateDequeue(task);
        return removeTaskInternal(task, cause);
    }

    private boolean removeTaskInternal(final TapeTask task, final String cause) {
        final boolean retval = m_sortableTaskList.remove(task);
        if (task instanceof IoTask) {
            final Collection<UUID> chunkIds =((IoTask)task).getChunkIds();
            final UUID[] jobIds = task.getJobIds();
            for (final UUID chunkId : chunkIds) {
                if (m_ioTasksByChunkId.containsKey(chunkId)) {
                    m_ioTasksByChunkId.get(chunkId).remove(task);
                } else {
                    LOG.warn("Removing task " + task.toString() + " for entry " + chunkId + " but no such task was found for that chunk ID.");
                }
            }
            for (final UUID jobId : jobIds) {
                if (m_ioTasksByJobId.containsKey(jobId)) {
                    m_ioTasksByJobId.get(jobId).remove(task);
                } else {
                    LOG.warn("Removing task " + task.toString() + " for job " + jobId + " but no such task was found for that job ID.");
                }
            }
        }
        dequeued(task, cause);
        return retval;
    }

    @Override
    public List<IoTask> getTasksForChunk(final UUID chunkId) {
        synchronized ( m_taskStateLock ) {
            final Collection<IoTask> tasksForChunk = m_ioTasksByChunkId.get(chunkId);
            if (tasksForChunk == null) {
                return new ArrayList<>();
            } else {
                return new ArrayList<>(tasksForChunk);
            }
        }
    }

    @Override
    public Set<IoTask> getTasksForJob(final UUID jobId) {
        synchronized ( m_taskStateLock ) {
            final Collection<IoTask> tasksForJob = m_ioTasksByJobId.get(jobId);
            if (tasksForJob == null) {
                return new HashSet<>();
            } else {
                return new HashSet<>(tasksForJob);
            }
        }
    }

    public boolean hasReadyTasks(final Predicate<TapeTask> predicate) {
        return m_sortableTaskList.stream().anyMatch(predicate.and((task) -> task.getState() == BlobStoreTaskState.READY));
    }

    private void validateDequeue(final TapeTask task) {
        if ( BlobStoreTaskState.PENDING_EXECUTION == task.getState()
                || BlobStoreTaskState.IN_PROGRESS == task.getState() )
        {
            throw new IllegalStateException(
                    "Cannot dequeue " + task + " while it is in state " + task.getState() + "." );
        }
    }

    private void dequeued( final TapeTask task, final String cause )
    {
        m_tapeTaskDequeuedListener.taskDequeued( task );
        LOG.info( "Dequeued " + task.toString() + " since " + cause + "." );
    }

    @Override
    public List<TapeTask> getSortedPendingTapeTasksFor(final Comparator< TapeTask > comparator, final Predicate<TapeTask> canExecuteFilter) {
        final Predicate<TapeTask> readyFilter = (task) -> task.getState().equals(BlobStoreTaskState.READY);
        synchronized ( m_taskStateLock ) {
            return m_sortableTaskList.stream().filter(
                    readyFilter.and(canExecuteFilter)
            ).sorted(comparator).collect(Collectors.toList());
        }
    }

    @Override
    public List<TapeTask> getAllTapeTasks() {
        synchronized ( m_taskStateLock ) {
            //TODO: consider ways to reduce overhead caused by copy here without sacrificing data hiding or concurrency
            return new ArrayList<>(m_sortableTaskList);
        }
    }

    @Override
    public Set<UUID> getChunkIds() {
        synchronized ( m_taskStateLock ) {
            return new HashSet<>(m_ioTasksByChunkId.keySet());
        }
    }

    @Override
    public List<TapeTask> get(final UUID tapeId) {
        synchronized ( m_taskStateLock ) {
            if (!m_staticTasksByTapeId.containsKey(tapeId)) return new ArrayList<>();
            return new ArrayList<>(m_staticTasksByTapeId.get(tapeId));
        }
    }

    @Override
    public boolean tryPriorityUpdate(
            final UUID tapeId,
            final Class<?> tapeTaskType,
            final BlobStoreTaskPriority newPriority,
            final boolean onlyChangePriorityIfIncreasingPriority,
            final boolean alwaysLogIfTaskExists)
    {
        synchronized ( m_taskStateLock ) {
            Validations.verifyNotNull("Tape", tapeId);
            Validations.verifyNotNull("Class", tapeTaskType);
            Validations.verifyNotNull("New priority", newPriority);
            final List<TapeTask> tasks = get(tapeId);
            boolean updatedAtLeastOneTask = false;
            for (final TapeTask task : tasks) {
                if (!tapeTaskType.isAssignableFrom(task.getClass())) {
                    // skip this task, its priority cannot be updated
                    continue;
                }
                final BlobStoreTaskPriority oldPriority = task.getPriority();
                if (!onlyChangePriorityIfIncreasingPriority
                        || newPriority.ordinal() < task.getPriority().ordinal()) {
                    task.setPriority(newPriority);
                }
                if (oldPriority == newPriority) {
                    if (alwaysLogIfTaskExists) {
                        LOG.info("Already scheduled " + task + " at priority " + task.getPriority() + ".");
                    }
                } else {
                    LOG.info("Already scheduled " + task + " changed priority from "
                            + oldPriority + " to " + task.getPriority() + ".");
                }
                updatedAtLeastOneTask = true;
            }
            return updatedAtLeastOneTask;
        }
    }


    @Override
    public boolean remove(final TapeTask tapeTask, final String cause) {
        synchronized ( m_taskStateLock ) {
            if (tapeTask instanceof StaticTapeTask) {
                return removeTask((StaticTapeTask) tapeTask, cause);
            } else if (tapeTask instanceof DynamicTapeTask) {
                return removeTask((DynamicTapeTask) tapeTask, cause);
            } else {
                throw new IllegalStateException("Task must be either a static or dynamic task.");
            }
        }
    }

    @Override
    public int size() {
        synchronized ( m_taskStateLock ) {
            return m_sortableTaskList.size();
        }
    }


    public void removeAllIoTasks(final String reason) {
        synchronized (m_taskStateLock) {
            m_sortableTaskList.stream()
                    .filter((task) -> task instanceof IoTask && task.getState() == BlobStoreTaskState.READY).forEach((task) -> {
                        remove(task, reason);
                    });
        }
    }


    private final List<TapeTask> m_sortableTaskList = new LinkedList<>();
    private final TapeTaskDequeuedListener m_tapeTaskDequeuedListener;
    private final Multimap<UUID, StaticTapeTask> m_staticTasksByTapeId = MultimapBuilder.hashKeys().arrayListValues().build();
    private final Multimap<UUID, IoTask> m_ioTasksByChunkId = MultimapBuilder.hashKeys().hashSetValues().build();
    private final Multimap<UUID, IoTask> m_ioTasksByJobId = MultimapBuilder.hashKeys().hashSetValues().build();
    private final Object m_taskStateLock;
    private final static Logger LOG = Logger.getLogger(TapeTaskQueueImpl.class);
}

