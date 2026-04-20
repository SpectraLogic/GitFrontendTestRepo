/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.TapeFailureNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.domain.shared.ReservedTaskType;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.orm.BlobLocalDestinationRM;
import com.spectralogic.s3.common.dao.orm.BlobLocalTargetRM;
import com.spectralogic.s3.common.dao.orm.StorageDomainRM;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.notification.generator.TapeFailureNotificationPayloadGenerator;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.frmwrk.*;
import com.spectralogic.s3.dataplanner.backend.tape.api.*;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.Refresh;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeMoveStrategy;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy.LoadTapeIntoDriveTapeMoveStrategy;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy.UnloadTapeFromDriveTapeMoveStrategy;
import com.spectralogic.s3.dataplanner.backend.tape.task.*;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.notification.domain.bean.HttpNotificationEvent;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.ThrottledRunnable;
import com.spectralogic.util.thread.ThrottledRunnableExecutor;
import com.spectralogic.util.thread.ThrottledRunnableExecutor.WhenAggregating;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class TapeBlobStoreProcessorImpl extends BaseShutdownable implements TapeBlobStoreProcessor
{
    public TapeBlobStoreProcessorImpl(
            final TapeLockSupport<Object> tapeLockSupport,
            final BeansServiceManager serviceManager,
            final TapeEnvironment tapeEnvironment,
            final TapeFailureManagement tapeFailureManagement,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager)
    {
        Validations.verifyNotNull( "Tape lock support", tapeLockSupport );
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Tape environment", tapeEnvironment );
        Validations.verifyNotNull( "Tape failure management", tapeFailureManagement );
        
        m_tapeLockSupport = tapeLockSupport;
        m_serviceManager = serviceManager;
        m_tapeEnvironment = tapeEnvironment;
        m_tapeFailureManagement = tapeFailureManagement;
        m_diskManager = diskManager;
        m_jobProgressManager = jobProgressManager;

        m_tapeTasks = new TapeTaskQueueImpl(new TapeTaskDequeuedListenerImpl(), m_taskStateLock);
        m_tapeTaskStarter = new TapeTaskStarter();
        m_tapeEnvironment.addSchedulingListener( m_tapeTaskStarter );
        m_periodicTaskSchedulingExecutor = 
                new RecurringRunnableExecutor( m_tapeTaskStarter, 1 * 60 * 1000 );
        addShutdownListener( m_periodicTaskSchedulingExecutor );
        m_oldTapeFailuresDeleterExecutor.start();
    }
    
    
    private final class OldTapeFailuresDeleter implements Runnable
    {
        public void run()
        {
            m_serviceManager.getService( TapeFailureService.class ).deleteOldFailures();
        }
    } // end inner class def
    
    
    public void start()
    {
        m_periodicTaskSchedulingExecutor.start();
    }
    
    
    private final class TapeTaskStarter implements Runnable, BlobStoreTaskSchedulingListener
    {
        public void run()
        {
            m_eventDrivenTaskSchedulingRequiredExecutor.add( m_worker );
        }
        
        private void scheduleInspectTasks()
        {
            final Set< TapeState > stateCannotInspect = TapeState.getStatesThatAreNotPhysicallyPresent();
            stateCannotInspect.add( TapeState.INCOMPATIBLE );
            final Set< Tape > tapesPendingInspection = m_serviceManager.getRetriever( Tape.class )
                    .retrieveAll( Require.all(
                            Require.not( Require.beanPropertyEqualsOneOf( Tape.STATE, stateCannotInspect ) ),
                            Require.beanPropertyEquals( Tape.STATE, TapeState.PENDING_INSPECTION ) ) ).toSet();
            for ( final Tape tape : tapesPendingInspection )
            {
                scheduleInspection( BlobStoreTaskPriority.LOW, tape.getId(), false );
            }
        }
        
        private void scheduleDriveCleaningTasks()
        {
            for ( final UUID driveId : m_tapeEnvironment.getDrivesRequiringCleaning() )
            {
                final String result = scheduleCleaning( driveId );
                if ( null == result )
                {
                    LOG.info( "Cleaning scheduled for drive " + driveId + "." );
                }
                else
                {
                    LOG.info( "Cleaning could not be scheduled for drive " 
                              + driveId + " at this time: " + result );
                }
            }
        }
        
        public void taskSchedulingRequired( final BlobStoreTask task )
        {
            m_eventDrivenTaskSchedulingRequiredExecutor.add( m_worker );
            for ( final BlobStoreTaskSchedulingListener listener : m_taskSchedulingListeners )
            {
                listener.taskSchedulingRequired( task );
            }
        }
        
        private final class TapeTaskStarterWorker implements ThrottledRunnable
        {
            @Override
            public void run( final RunnableCompletionNotifier completionNotifier )
            {
                try
                {
                    Thread.currentThread().setName( TapeTaskStarter.class.getSimpleName() );
                    scheduleInspectTasks();
                    scheduleDriveCleaningTasks();
                    TapeBlobStoreProcessorImpl.this.run();
                }
                finally
                {
                    completionNotifier.completed();
                }
            }
        } // end inner inner class def
        private final TapeTaskStarterWorker m_worker = new TapeTaskStarterWorker();
        private final ThrottledRunnableExecutor< TapeTaskStarterWorker > 
            m_eventDrivenTaskSchedulingRequiredExecutor = 
            new ThrottledRunnableExecutor<>( 20, null, WhenAggregating.DELAY_EXECUTION );
    } // end inner class def
    
    
    public String scheduleInspection( final BlobStoreTaskPriority priority, final UUID tapeId, final boolean force )
    {
        synchronized ( m_taskStateLock )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            if ( !tape.getType().canContainData() )
            {
                return tape.getType() + " cannot contain data";
            }
            if ( !tape.getState()
                      .isPhysicallyPresent() || ( TapeState.INCOMPATIBLE == tape.getState() ) )
            {
                return "tape is in state " + tape.getState();
            }
            if ( !tape.getState().isLoadIntoDriveAllowed() )
            {
                if ( force || tape.getState().isInspectionAllowed() )
                {
                    m_serviceManager.getService( TapeService.class ).transistState(
                            tape, TapeState.PENDING_INSPECTION );
                    return scheduleInspection( priority, tapeId, false );
                }
                return "tape is in state " + tape.getState();
            }

            final List<TapeTask> existingTasks = m_tapeTasks.get( tape.getId() );
            for (final TapeTask existingTask : existingTasks)
            {
                if (existingTask instanceof InspectTapeTask && BlobStoreTaskState.READY != existingTask.getState())
                {
                    return "inspection in progress via " + existingTask;
                }
            }
            if ( m_tapeTasks.tryPriorityUpdate(
                    tape.getId(), InspectTapeTask.class, priority, !force, force ) )
            {
                return null;
            }
            m_tapeTasks.addStaticTask(new InspectTapeTask( priority, tape.getId(), m_tapeFailureManagement, m_serviceManager ) );
            return null;
        }
    }

    public String scheduleCleaning( final UUID tapeDriveId ) {
        return scheduleCleaningInternal(tapeDriveId, null);
    }

    private String scheduleCleaningInternal( final UUID tapeDriveId, final BlobStoreTaskSchedulingListener taskCompleteListener )
    {
        Validations.verifyNotNull( "Tape drive id", tapeDriveId );
        synchronized ( m_taskStateLock )
        {
            for ( final TapeTask task : m_tapeTasks.getAllTapeTasks() )
            {
                if ( !CleanTapeDriveTask.class.isAssignableFrom( task.getClass() ) )
                {
                    continue;
                }
                if ( ( (CleanTapeDriveTask)task ).getTapeDriveToClean().equals( tapeDriveId ) )
                {
                    return null;
                }
            }
            
            boolean atLeastOneEligibleCleaningTape = false;
            final TapeDrive drive = m_serviceManager.getRetriever( TapeDrive.class ).attain( tapeDriveId );
            for ( final Tape tape : m_serviceManager.getRetriever( Tape.class ).retrieveAll( Require.all( 
                    Require.beanPropertyEquals( Tape.PARTITION_ID, drive.getPartitionId() ),
                    Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ),
                    Require.beanPropertyEquals( Tape.TYPE, drive.getType().getCleaningTapeType() ) ) )
                                .toSet() )
            {
                atLeastOneEligibleCleaningTape = true;
                if ( m_tapeTasks.get( tape.getId() ).isEmpty() )
                {
                    final CleanTapeDriveTask task = new CleanTapeDriveTask( drive, tape.getId(), m_tapeFailureManagement, m_serviceManager );
                    if (taskCompleteListener != null) {
                        task.addSchedulingListener(taskCompleteListener);
                    }
                    m_tapeTasks.addStaticTask(task);
                    return null;
                }
            }
            
            if ( atLeastOneEligibleCleaningTape )
            {
                return "All eligible cleaning tapes have been scheduled to clean drives.  Try again later.";
            }
            m_serviceManager.getService( TapePartitionFailureService.class ).create(
                    drive.getPartitionId(), 
                    TapePartitionFailureType.CLEANING_TAPE_REQUIRED, "A cleaning tape of type " +
                            drive.getType().getCleaningTapeType() + " is required to clean drive " +
                            drive.getSerialNumber() + " (MFG SN " + drive.getMfgSerialNumber() + ").", 24 * 60 );
            return "No eligible tapes of type " + drive.getType().getCleaningTapeType()
                    + " exist to clean drive " + drive.getSerialNumber() + " (MFG SN " + drive.getMfgSerialNumber() + ").";
        }
    }


    @Override
    public void driveDump(final UUID driveId) {
        final Object lockHolder = "Dump request - " + UUID.randomUUID();
        final TapeDriveResource tapeDriveResource = m_tapeLockSupport.forceLock( driveId, lockHolder );
        try
        {
            tapeDriveResource.driveDump().get(RpcFuture.Timeout.LONG);
        }
        catch (final RpcProxyException ex) {
            throw new DataPlannerException(
                    GenericFailure.INTERNAL_ERROR,
                    "Failed to perform dump of drive " + driveId + ".", ex);
        }
        finally
        {
            m_tapeLockSupport.unlock(lockHolder);
        }
    }


    public String scheduleTest(final UUID tapeDriveId, final UUID tapeId, boolean cleanFirst)
    {
        final TapeDrive drive = m_serviceManager.getRetriever( TapeDrive.class ).attain( tapeDriveId );
        final TapePartitionFailureService failureService = m_serviceManager.getService( TapePartitionFailureService.class );
        if (cleanFirst) {
            final String scheduleCleaningError = scheduleCleaningInternal(tapeDriveId, task -> {
                if (task instanceof CleanTapeDriveTask) {
                    final TapeFailureService tapeFailureService = m_serviceManager.getService( TapeFailureService.class );
                    final TapeFailure mostRecentEventForDrive = tapeFailureService.retrieveMostRecentFailure(
                            Require.beanPropertyEquals(TapeFailure.TAPE_DRIVE_ID, drive.getId()));
                    if (mostRecentEventForDrive.getType() == TapeFailureType.DRIVE_CLEAN_FAILED) {
                        LOG.warn("Will not test " + tapeDriveId + ", because we failed when trying to clean it.");
                        failureService.create(
                                drive.getPartitionId(),
                                TapePartitionFailureType.TAPE_DRIVE_NOT_CLEANED,
                                "Will not test " + drive.getSerialNumber() + ", because we failed when trying to clean it.",
                                null );
                    } else {
                        if (mostRecentEventForDrive.getType() == TapeFailureType.CLEANING_TAPE_EXPIRED) {
                            LOG.warn("Was unable to clean " + tapeDriveId + " due to expired cleaning tape. Will proceed with test anyway.");
                            failureService.create(
                                    drive.getPartitionId(),
                                    TapePartitionFailureType.TAPE_DRIVE_NOT_CLEANED,
                                    "Was unable to clean " + drive.getSerialNumber() + " due to expired cleaning tape. Will proceed with test anyway.",
                                    null );
                        } else if (mostRecentEventForDrive.getType() != TapeFailureType.DRIVE_CLEANED) {
                            //This should not happen, but we log it just in case for debugging purposes.
                            LOG.warn("There was an error determining result of cleaning drive" + tapeDriveId + ". Will proceed with test anyway.");
                            failureService.create(
                                    drive.getPartitionId(),
                                    TapePartitionFailureType.TAPE_DRIVE_NOT_CLEANED,
                                    "There was an error determining result of cleaning drive" + drive.getSerialNumber() + ". Will proceed with test anyway.",
                                    null );
                        }
                        String testOutput = scheduleTestInternal(drive, tapeId);
                        if (testOutput != null) {
                            final TapeFailure tapeFailure = BeanFactory.newBean( TapeFailure.class );
                            tapeFailure.setDate( new Date() );
                            tapeFailure.setErrorMessage( testOutput);
                            tapeFailure.setTapeDriveId( drive.getId()  );
                            tapeFailure.setTapeId( tapeId);
                            tapeFailure.setType( TapeFailureType.DRIVE_TEST_FAILED );
                            final TapeFailureService service =
                                    m_serviceManager.getService( TapeFailureService.class );
                            service.create(tapeFailure);
                            LOG.warn("Failed to schedule a drive test for drive " + tapeDriveId );

                            m_serviceManager.getNotificationEventDispatcher().fire(
                                    new HttpNotificationEvent(
                                            m_serviceManager.getRetriever( TapeFailureNotificationRegistration.class ),
                                            new TapeFailureNotificationPayloadGenerator( tapeFailure ) ) );

                        }
                    }
                }
            });
            if (scheduleCleaningError == null) {
                return null;
            } else {
                LOG.warn("Failed to schedule a drive clean for drive " + tapeDriveId + ", will attempt drive test without cleaning.");
                failureService.create(
                        drive.getPartitionId(),
                        TapePartitionFailureType.TAPE_DRIVE_NOT_CLEANED,
                        "Failed to schedule a drive clean for drive " + drive.getSerialNumber() + ", will attempt drive test without cleaning.",
                        null );
                return scheduleTestInternal(drive, tapeId);
            }
        } else {
            return scheduleTestInternal(drive, tapeId);
        }
    }

    private String scheduleTestInternal(final TapeDrive drive, final UUID tapeId)
    {
        Validations.verifyNotNull( "Tape drive", drive );
        synchronized ( m_taskStateLock )
        {
            TapeTask taskToOverwrite = null;
            for ( final TapeTask task : m_tapeTasks.getAllTapeTasks() )
            {
                if ( !TestTapeDriveTask.class.isAssignableFrom( task.getClass() ) )
                {
                    continue;
                }
                if ( ( (TestTapeDriveTask)task ).getTapeDriveToTest().equals( drive.getId() ) )
                {
                    if (tapeId == null || tapeId.equals(task.getTapeId())) {
                        return null;
                    }
                    taskToOverwrite = task;
                }
            }

            final WhereClause tapeIdFilter = tapeId == null ? Require.nothing() : Require.beanPropertyEquals(Tape.ID, tapeId);

            boolean atLeastOneEligibleTestTape = false;
            for ( final Tape tape : m_serviceManager.getRetriever( Tape.class ).retrieveAll( Require.all(
                            Require.beanPropertyEquals( Tape.PARTITION_ID, drive.getPartitionId() ),
                            Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ),
                            Require.beanPropertyEquals( Tape.ROLE, TapeRole.TEST),
                            tapeIdFilter) )
                    .toSet() )
            {
                atLeastOneEligibleTestTape = true;
                if ( m_tapeTasks.get( tape.getId() ).isEmpty() )
                {
                    if (taskToOverwrite != null) {
                        if ( BlobStoreTaskState.READY != taskToOverwrite.getState() )
                        {
                            throw new DataPlannerException(
                                    GenericFailure.CONFLICT,
                                    "There is already a test scheduled for this tape drive in state " + taskToOverwrite.getState() + "." );
                        }
                        //We only overwrite an existing task if the new request is valid and is for a different tape id than the current one
                        getTapeTasks().remove( taskToOverwrite, "different test tape requested" );
                    }
                    m_tapeTasks.addStaticTask(new TestTapeDriveTask( drive, tape.getId(), m_tapeFailureManagement, m_serviceManager ) );
                    return null;
                }
            }

            if ( atLeastOneEligibleTestTape )
            {
                if (tapeId != null) {
                    final Tape tape = m_serviceManager.getRetriever( Tape.class ).retrieve( tapeId );
                    final String tapeIdentification = tape != null ? tape.getBarCode() : tapeId.toString();
                    return "Tape " + tapeIdentification + " is not currently available to test drive. Try again later.";
                }
                return "All eligible test tapes have been scheduled to test drives.  Try again later.";
            }
            if (tapeId != null) {
                final Tape tape = m_serviceManager.getRetriever( Tape.class ).retrieve( tapeId );
                final String tapeIdentification = tape != null ? tape.getBarCode() : tapeId.toString();
                return "Tape " + tapeIdentification + " has become unavailable for testing.";
            }
            return "No eligible tapes of role " + TapeRole.TEST
                    + " exist to test drive " + drive.getMfgSerialNumber() + ".";
        }
    }
    
    
    synchronized private void run()
    {
        final Duration duration = new Duration();
        final String msg = "Running " + TapeTaskStarter.class.getSimpleName();
        final MonitoredWork work = new MonitoredWork( StackTraceLogging.LONG, msg );
        try
        {
            runInternal();
        }
        finally
        {
            LOG.info( "Completed running " 
                      + TapeTaskStarter.class.getSimpleName() + " in " + duration + "." );
            work.completed();
        }
    }
    

    private void runInternal()
    {
        if ( !m_tapeEnvironment.ensurePhysicalTapeEnvironmentUpToDate( Refresh.ALWAYS_CHECK_GEN_NUM ) )
        {
            return;
        }
        
        m_tapeLockSupport.ensureAvailableTapeDrivesAreUpToDate(this::moveTapeOutOfIdleDrive);
        cleanUpCompletedTasks();
        
        moveTapesOutOfDrivesAndPartitionsBeingQuiesced();
        
        if ( m_tapeLockSupport.getAvailableTapeDrives().isEmpty() )
        {
        	LOG.info( "All tape drives are currently busy or unavailable." );
            return;
        }

        final List<BlobStoreTaskPriority> minPrioritiesToExecute = new ArrayList<>();
        LOG.info("Tape tasks are currently running or queued:\n"
                + TapeTaskQueue.getTaskSummaryByState(m_tapeTasks.getAllTapeTasks()));

        /**
         * NOTE: The tape task comparator is adequate to make sure critical and urgent tasks always
         * take priority over other tasks for a given drive, but we still must do multiple passes with
         * a minimum priority to ensure each critical or urgent task gets executed on the best drive
         * available (e.g. one with the appropriate tape already in it) instead of just the first drive
         * we loop over.
         */

        //If there are any critical priorities we must process them all first
        if (anyReadyTasksOfPriority(BlobStoreTaskPriority.CRITICAL)) {
            minPrioritiesToExecute.add(BlobStoreTaskPriority.CRITICAL);
        }
        //If there are any urgent priorities we must process them all before any lower priorities
        if (anyReadyTasksOfPriority(BlobStoreTaskPriority.URGENT)) {
            minPrioritiesToExecute.add(BlobStoreTaskPriority.URGENT);
        }
        //Only process for less than urgent if we have any such tasks
        if (anyRegularPriorityTasksReady()) {
            minPrioritiesToExecute.add(BlobStoreTaskPriority.BACKGROUND);
        }

        if (minPrioritiesToExecute.isEmpty()) {
            LOG.info( "No new tasks ready for execution." );
            cleanUpIoTasksThatNoLongerApply();
            return;
        }

        verifyPartitionsCanServiceGetsAndVerifies();
        for ( final BlobStoreTaskPriority minPriority : minPrioritiesToExecute )
        {
            final String qualifier = minPriority == BlobStoreTaskPriority.BACKGROUND ? "" : minPriority.toString() + " ";
            //NOTE: Here we clear any tapes that were "recently" unlocked. This matters only when starting tasks where
            //allowTapeMove is true. We do not want the "moving" pass to consider tapes that became unlocked while it
            //was processing, since we might not yet have considered them for a "non-moving" pass. See also: notes in
            //TapeAvailabilityImpl.
            m_tapeLockSupport.clearRecentlyUnlocked();
            LOG.info( "Starting " + qualifier + "tape tasks that do not require moves..." );
            startTasks( minPriority, false );
            LOG.info( "Starting " + qualifier + "tape tasks that may require moves..." );
            startTasks( minPriority, true );
        }
    }


    private boolean anyReadyTasksOfPriority(final BlobStoreTaskPriority exactPriority) {
        return m_tapeTasks.hasReadyTasks((task) -> task.getPriority() == exactPriority) ||
                TapeWorkAggregationUtils.anyTapeIOWorkOfPriority(m_serviceManager, exactPriority);
    }


    private boolean anyRegularPriorityTasksReady() {
        return m_tapeTasks.hasReadyTasks((task) -> task.getPriority().isLowerPriorityThan(BlobStoreTaskPriority.URGENT)) ||
                TapeWorkAggregationUtils.anyRegularPriorityTapeIOWork(m_serviceManager);
    }
    
    
    private void moveTapesOutOfDrivesAndPartitionsBeingQuiesced()
    {
        for ( final TapeDrive drive : m_serviceManager.getRetriever( TapeDrive.class ).retrieveAll( 
                Require.all( 
                        //has a tape
                        Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ),
                        Require.any(
                            //drive being quiesced
                            Require.beanPropertyEquals(
                                    TapeDrive.QUIESCED, 
                                    Quiesced.PENDING ),
                            //drive in partition being quiesced
                            Require.exists(
                                    TapeDrive.PARTITION_ID, 
                                    Require.beanPropertyEquals(
                                            TapePartition.QUIESCED, 
                                            Quiesced.PENDING ) ) ) ) ).toSet() )
        {
            if ( TapeDriveState.OFFLINE == drive.getState() || TapeDriveState.ERROR == drive.getState() )
            {
                LOG.warn( "Tape drive " + drive.getId() + " in partition " + drive.getPartitionId()
                          + " is " + drive.getState() + " and cannot have its tape (" + drive.getTapeId() 
                          + ") removed in order to perform drive/partition quiesce." );
                continue;
            }

            if (drive.getPartitionId() != null) {
                final TapePartition partition = m_serviceManager.getRetriever(TapePartition.class).attain(drive.getPartitionId());
                if (TapePartitionState.ONLINE != partition.getState()) {
                    LOG.warn( "Tape partition " + partition.getId() + " is not ONLINE but has state " + partition.getState()
                            + " and cannot have its tape (" + drive.getTapeId() + ") removed." );
                    continue;
                }
            }
            
            final TapeDriveResource tdResource;
            final TapeTask lockHolder = InterfaceProxyFactory.getProxy( 
                    TapeTask.class, 
                    MockInvocationHandler.forToString(
                    "Move tape out of drive/partition being quiesced" ) );
            LOG.info( "Tape " + drive.getTapeId() + " in drive " + drive.getId() 
                    + ", partition " + drive.getPartitionId()
                    + " must be removed in order perform quiesce drive/partition." );
            try
            {
                tdResource = m_tapeLockSupport.forceLock( drive.getId(), lockHolder );
            }
            catch ( final RuntimeException ex )
            {
                LOG.info( "Failed to remove tape from drive to quiesce drive/partition at this time.", ex );
                continue;
            }
            
            try
            {
                final TapePartition partition = 
                        m_serviceManager.getRetriever( TapePartition.class ).attain( drive.getPartitionId() );
                m_tapeEnvironment.performMove(
                        tdResource,
                        partition.getSerialNumber(),
                        m_serviceManager.getRetriever( Tape.class ).attain( drive.getTapeId() ),
                        new UnloadTapeFromDriveTapeMoveStrategy( drive, ElementAddressType.STORAGE ), 
                        false );
                LOG.info( "Successfully removed tape from drive " + drive.getId() + "." );
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( "Failed to remove tape from drive to quiesce drive/partition at this time.", ex );
                continue;
            }
            finally
            {
                m_tapeLockSupport.unlock( lockHolder );
            }
        }
    }


    private Boolean moveTapeOutOfIdleDrive(final UUID driveId)
    {
        final TapeDrive drive = m_serviceManager.getRetriever(TapeDrive.class).attain(driveId);
        if (drive.getTapeId() == null) return true;

        if ( TapeDriveState.OFFLINE == drive.getState() || TapeDriveState.ERROR == drive.getState() )
        {
            LOG.warn( "Tape drive " + drive.getId() + " in partition " + drive.getPartitionId()
                    + " is idle, but is in state" + drive.getState() + " and cannot have its tape ("
                    + drive.getTapeId() + ") removed." );
            return false;
        }

        if (drive.getPartitionId() != null) {
            final TapePartition partition = m_serviceManager.getRetriever(TapePartition.class).attain(drive.getPartitionId());
            if (TapePartitionState.ONLINE != partition.getState() || Quiesced.NO != partition.getQuiesced()) {
                LOG.warn( "Tape partition " + partition.getId() + " is either not ONLINE (" + partition.getState()
                        + ") OR is not quiesced (" + partition.getQuiesced() + ") and cannot have its tape ("
                        + drive.getTapeId() + ") removed." );
                return false;
            }
        }

        final TapeDriveResource tdResource;
        final TapeTask lockHolder = InterfaceProxyFactory.getProxy(
                TapeTask.class,
                MockInvocationHandler.forToString(
                        "Move tape out of idle drive" ) );
        LOG.info( "Tape " + drive.getTapeId() + " in drive " + drive.getId()
                + ", partition " + drive.getPartitionId()
                + " will be removed since it is idle." );
        try
        {
            tdResource = m_tapeLockSupport.lock( drive.getId(), lockHolder );
        }
        catch ( final RuntimeException ex )
        {
            LOG.info( "Failed to remove tape from idle drive at this time.", ex );
            return false;
        }

        try
        {
            if (drive.getTapeId() != null) {
                m_tapeLockSupport.addTapeLock(lockHolder, drive.getTapeId());
            }
            final TapePartition partition =
                    m_serviceManager.getRetriever( TapePartition.class ).attain( drive.getPartitionId() );
            m_tapeEnvironment.performMove(
                    tdResource,
                    partition.getSerialNumber(),
                    m_serviceManager.getRetriever( Tape.class ).attain( drive.getTapeId() ),
                    new UnloadTapeFromDriveTapeMoveStrategy( drive, ElementAddressType.STORAGE ),
                    false );
            LOG.info( "Successfully removed tape from idle drive " + drive.getId() + "." );
            return true;
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to remove tape from idle drive at this time.", ex );
            return false;
        }
        finally
        {
            m_tapeLockSupport.unlock( lockHolder );
        }
    }
    
    
    private void cleanUpCompletedTasks()
    {
        synchronized ( m_taskStateLock )
        {
            final Set< Object > tasksWithLocks = m_tapeLockSupport.getAllLockHolders();
            cleanUpCompletedTasks( m_tapeTasks, tasksWithLocks );
        }
    }
    

    private void cleanUpCompletedTasks(
            final TapeTaskQueueImpl tasks,
            final Set< Object > tasksWithLocks )
    {
        for ( final TapeTask task : tasks.getAllTapeTasks() )
        {
            if ( ( BlobStoreTaskState.NOT_READY == task.getState() 
                    || BlobStoreTaskState.READY == task.getState() )
                 && tasksWithLocks.contains( task ) )
            {
                m_tapeLockSupport.unlock( task );
            }
            if ( BlobStoreTaskState.COMPLETED == task.getState() )
            {
                tasks.remove( task, "it has completed" );
            }
        }
    }

    
    private void handleTapeDriveRequiringTapeRemoval( 
            final TapeDrive tapeDrive,
            final boolean tapeAlreadyLocked )
    {
        cleanUpCompletedTasks();
        
        if ( null == tapeDrive.getTapeId() )
        {
            LOG.warn( "No tape in drive " + tapeDrive 
                      + ".  It is illegal to force tape removal on a drive that has no tape.  " 
                      + "Cannot proceed." ); 
            return;
        }
        
        final TapeTask mockTask = InterfaceProxyFactory.getProxy( 
                TapeTask.class, 
                MockInvocationHandler.forToString( "Remove tape from drive requiring removal" ) );
        final TapePartition partition = 
                m_serviceManager.getRetriever( TapePartition.class ).attain( tapeDrive.getPartitionId() );
        final TapeDriveResource tdResource;
        try
        {
            tdResource = m_tapeLockSupport.lock( tapeDrive.getId(), mockTask );
        }
        catch ( final RuntimeException ex )
        {
            LOG.info( "Must retry later on tape drive: " + tapeDrive, ex );
            return;
        }
        
        try
        {
            if ( !tapeAlreadyLocked )
            {
                m_tapeLockSupport.addTapeLock( mockTask, tapeDrive.getTapeId() );
            }
            m_tapeEnvironment.performMove(
                    tdResource,
                    partition.getSerialNumber(),
                    m_serviceManager.getRetriever( Tape.class ).attain( tapeDrive.getTapeId() ),
                    new UnloadTapeFromDriveTapeMoveStrategy( tapeDrive, ElementAddressType.STORAGE ),
                    false );
            m_tapeEnvironment.flagForQuiesceStateAndRefresh();
            m_tapeTaskStarter.taskSchedulingRequired( null );
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Must retry later on tape drive: " + tapeDrive, ex );
        }
        finally
        {
            m_tapeLockSupport.unlock( mockTask );
        }
    }
    
    
    private void verifyPartitionsCanServiceGetsAndVerifies()
    {
        for ( final TapePartition partition 
                : m_serviceManager.getRetriever( TapePartition.class ).retrieveAll().toSet() )
        {
            m_offlinePartitionTracker.update( 
                    partition.getId(), 
                    TapePartitionState.ONLINE != partition.getState()
                    || Quiesced.NO != partition.getQuiesced() );
        }
        
        synchronized ( m_taskStateLock )
        {
            final Set< TapeTask > tasks = m_tapeTasks.getAllTapeTasks().stream().filter((task) -> {
                return ChunkReadingTask.class.isAssignableFrom(task.getClass());
            }).collect(Collectors.toSet());

            final Set< UUID > tapeIds = new HashSet<>();
            for ( final TapeTask t : tasks )
            {
                tapeIds.add( ( (ChunkReadingTask)t ).getEntries().iterator().next().getReadFromTapeId() );
            }
            
            final Map< UUID, Tape > tapes = BeanUtils.toMap( 
                    m_serviceManager.getRetriever( Tape.class ).retrieveAll( tapeIds ).toSet() );
            for ( final TapeTask t : tasks )
            {
                final JobEntry chunk = ( (ChunkReadingTask)t ).getEntries().iterator().next();
                final Tape tape = tapes.get( chunk.getReadFromTapeId() );
                UUID partitionId = tape.getPartitionId();
                if (partitionId == null) {
                    LOG.warn("Tape " + tape.getBarCode() + " is not currently in a tape partition, so" +
                            " task " + t + " cannot read from it.");
                    deleteTask(t);
                    continue;
                }
                final TapePartition partition = m_serviceManager.getRetriever(TapePartition.class).attain( partitionId );
                if ( Quiesced.NO != partition.getQuiesced() )
                {
                    partitionCannotServiceGetOrVerify( t, partition, TapePartition.QUIESCED );
                }
                if ( TapePartitionState.ONLINE != partition.getState() )
                {
                    partitionCannotServiceGetOrVerify( t, partition, TapePartition.STATE );
                }
            }
        }
    }
    
    
    private void partitionCannotServiceGetOrVerify(
            final TapeTask t,
            final TapePartition partition,
            final String beanProperty )
    {
        final DataPathBackend dpb =
                m_serviceManager.getRetriever( DataPathBackend.class ).attain( Require.nothing() );
        final Duration durationOffline = m_offlinePartitionTracker.getOfflineDuration( partition.getId() );
        final boolean fail = ( null != durationOffline && durationOffline.getElapsedMinutes()
                > dpb.getUnavailableTapePartitionMaxJobRetryInMins() );
        final String suffix = ( fail ) ?
                "Must re-chunk since partition has been unavailable for " + durationOffline 
                : "Will wait to see if re-chunking can be avoided since partition has only been " 
                  + "unavailable for " + durationOffline;
        try
        {
            LOG.warn( "Tape partition " + partition.getName() + " is " + beanProperty + "=" 
                    + BeanUtils.getReader( TapePartition.class, beanProperty ).invoke( partition )
                    + ", so cannot service " + t.getName() + ".  "
                    + suffix + "." );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        if ( fail )
        {
            deleteTask( t );
        }
    }
    
        
    private void startTasks(final BlobStoreTaskPriority minPriority, boolean allowTapeMove)
    {
        final Set< UUID > drivesRequiringCleaning = m_tapeEnvironment.getDrivesRequiringCleaning();
        if ( !drivesRequiringCleaning.isEmpty() )
        {
            LOG.info( "Tape drives requiring cleaning: " + drivesRequiringCleaning );
        }
        final List< TapeDrive > availableTapeDrives = getSortedAvailableTapeDrives();

        final Map<UUID, Long> availableTapeDriveCountByPartition =
                availableTapeDrives.stream().collect(Collectors.groupingBy((drive) -> drive.getPartitionId(), Collectors.counting()));

        for ( final TapeDrive drive : availableTapeDrives )
        {
            if ( null == drive )
            {
                continue;
            }
            if ( m_tapeEnvironment.isTaskExecutionSuspended( drive.getPartitionId() ) )
            {
                LOG.info( "Will not attempt to execute a task for tape drive " + drive.getId()
                          + " since tape moves are temporarily suspended on partition "
                          + drive.getPartitionId() + " due to recent move failures." );
                allowTapeMove = false;
            }

            final TapePartition partition =
                    m_serviceManager.getRetriever( TapePartition.class ).attain( drive.getPartitionId() );

            if ( allowTapeMove && m_tapeEnvironment.tryPartitionLock( partition.getSerialNumber() ) ) {
                LOG.info( "Tape drive '" + drive.getSerialNumber() + "' can start '" + drive.getReservedTaskType().toString() +"' tasks." );
            } else if ( drive.getTapeId() == null) {
                continue;
            } else {
                allowTapeMove = false;
                LOG.info( "Tape drive '" + drive.getSerialNumber() + "' can start '" + drive.getReservedTaskType().toString() +"' tasks using tape " + drive.getTapeId() + "." );
            }
            try
            {
                synchronized ( m_startTasksLock )
                {
                    final TapeAvailability tapeAvailability;
                    final List< TapeTask > nextTasks;
                    synchronized ( m_taskStateLock )
                    {
                        tapeAvailability = getTapeAvailability( drive, allowTapeMove );
                        nextTasks = getNextTasks(minPriority, tapeAvailability, allowTapeMove);
                    }
                    if (nextTasks.isEmpty()) {
                        LOG.info("No tasks will be considered for execution on drive " + drive.getSerialNumber() + ".");
                        continue;
                    } else {
                        LOG.info("Will consider " + nextTasks.size() + " tasks for execution on drive " + drive.getSerialNumber() + ":\n"
                                + TapeTaskQueue.getTaskSummary(nextTasks));
                    }

                    long availableTapeDriveCount = 0;
                    if ( null != availableTapeDriveCountByPartition.get(partition.getId()) )
                    {
                        availableTapeDriveCount = availableTapeDriveCountByPartition.get(partition.getId());
                    }
                    int numberOfRunningReadTasks = 0;
                    int numberOfRunningWriteTasks = 0;

                    if ( 0 < partition.getMinimumReadReservedDrives() ||
                            0 < partition.getMinimumWriteReservedDrives() )
                    {
                        for ( final TapeTask task : m_tapeTasks.getAllTapeTasks() )
                        {
                            final UUID driveId = task.getDriveId();
                            if ( null == driveId )
                            {
                                continue;
                            }
                            final TapeDrive busyDrive = m_serviceManager.getRetriever(TapeDrive.class).retrieve(driveId);
                            if ( null == busyDrive || busyDrive.getPartitionId() != partition.getId() )
                            {
                                continue;
                            }

                            final Class<?> clazz = task.getClass();
                            if ( BlobStoreTaskState.IN_PROGRESS == task.getState()
                                    || BlobStoreTaskState.PENDING_EXECUTION == task.getState() )
                            {
                                if ( ReadChunkFromTapeTask.class.isAssignableFrom(clazz) )
                                {
                                    ++numberOfRunningReadTasks;
                                }
                                else if ( WriteChunkToTapeTask.class.isAssignableFrom(clazz) )
                                {
                                    ++numberOfRunningWriteTasks;
                                }
                            }
                        }
                    }

                    final HashMap<String, Integer> skipReasons = new HashMap<>();
                    for ( final TapeTask nextTask : nextTasks )
                    {
                        //TODO: consider removing, we should probably exclude drives that require cleaning from "available" status
                        if ( drivesRequiringCleaning.contains( drive.getId() )
                                && !CleanTapeDriveTask.class.isAssignableFrom( nextTask.getClass() ) )
                        {
                            tallyFilterReason(skipReasons, "drive requires cleaning");
                            continue;
                        }
                        final boolean maintenanceTask = CleanTapeDriveTask.class.isAssignableFrom( nextTask.getClass() )
                                    || TestTapeDriveTask.class.isAssignableFrom( nextTask.getClass() );
                        if (drive.getReservedTaskType() == ReservedTaskType.MAINTENANCE && !maintenanceTask) {
                            tallyFilterReason(skipReasons, "drive is reserved for maintenance tasks only");
                            continue;
                        }

                        final Class< ? > nextClazz = nextTask.getClass();
                        if ( ReadChunkFromTapeTask.class.isAssignableFrom( nextClazz ) )
                        {
                            /*
                             * Skip this read task if this is a write reserved drive, or there's a write
                             * reserved drive pool and there's not enough extra drives to start the read
                             * while still maintaining the minimum write reserved drive number.
                             */
                            if ( drive.getReservedTaskType() == ReservedTaskType.WRITE ||
                                     ( 0 < partition.getMinimumWriteReservedDrives() &&
                                       availableTapeDriveCount <= partition.getMinimumWriteReservedDrives() -
                                                                  numberOfRunningWriteTasks ) )
                            {
                                tallyFilterReason(skipReasons, "write reservations on drive or partition");
                                continue;
                            }
                        }
                        else if ( WriteChunkToTapeTask.class.isAssignableFrom( nextClazz ) )
                        {
                            if ( drive.getReservedTaskType() == ReservedTaskType.READ ||
                                    ( 0 < partition.getMinimumReadReservedDrives() &&
                                      availableTapeDriveCount <= partition.getMinimumReadReservedDrives() -
                                                                 numberOfRunningReadTasks ) )
                            {
                                tallyFilterReason(skipReasons, "read reservations on drive or partition");
                                continue;
                            }
                        }
                        if ( drive.getMinimumTaskPriority() != null
                                && nextTask.getPriority().isLowerPriorityThan(drive.getMinimumTaskPriority() ) ) {
                            tallyFilterReason(skipReasons, "priority minimum on drive");
                            continue;
                        }
                        try
                        {
                            final StartTaskResult startTaskResult;
                            synchronized ( m_taskStateLock )
                            {
                                if ( !stillActive( nextTask ) )
                                {
                                    tallyFilterReason(skipReasons, "task is no longer active");
                                    continue;
                                }

                                startTaskResult = startTask( nextTask, drive.getId(), tapeAvailability );
                            }
                            if ( StartTaskResult.FAILED != startTaskResult )
                            {
                                /*
                                 * If a tape move is required, we must wait until the tape changing task has
                                 * acquired the lock to perform the move before we proceed and release our
                                 * lock on it.  Tape changing task runners are the only blocking lockers, so
                                 * if we see anybody in the wait queue blocked, we can safely assume it's our
                                 * task runner.
                                 */
                                if ( StartTaskResult.STARTED_WITH_TAPE_MOVE == startTaskResult )
                                {
                                    final Duration duration = new Duration();
                                    while ( 0 == duration.getElapsedMinutes()
                                            && !m_tapeEnvironment.threadsWaitingForPartitionLock(partition.getSerialNumber() ) )
                                    {
                                        try
                                        {
                                            //TODO: looping wait here should probably use a blocking call or something else
                                            Thread.sleep( 1 );
                                        }
                                        catch ( final InterruptedException ex )
                                        {
                                            throw new RuntimeException( ex );
                                        }
                                    }
                                    if ( 0 < duration.getElapsedMinutes() )
                                    {
                                        LOG.warn( "Tape changing task runner never appeared to run for "
                                                  + nextTask + "." );
                                    }
                                }
                                --availableTapeDriveCount;
                                break;
                            }
                        }
                        catch ( final RuntimeException ex )
                        {
                            LOG.error( "Failed attempting to start task: " + nextTask, ex );
                            deleteTask( nextTask );
                        }
                    }
                    if (!skipReasons.isEmpty()) {
                        LOG.info("Some tasks under consideration were skipped: " + getFilterReport(skipReasons));
                    }
                }
            }
            finally
            {
                if ( allowTapeMove )
                {
                    m_tapeEnvironment.unlockPartition(partition.getSerialNumber());
                }
            }
        }
    }
    
    
    private List< TapeDrive > getSortedAvailableTapeDrives()
    {
        final List< TapeDrive > drives =
            m_serviceManager.getRetriever( TapeDrive.class ).retrieveAll(
            m_tapeLockSupport.getAvailableTapeDrives() ).toList();
        
        for ( int i = drives.size() - 1; i >= 0; i-- )
        {
            if ( drives.get( i ).isForceTapeRemoval() )
            {
                drives.remove( i );
            }
        }

        //NOTE: we shuffle before sorting here so that "ties" don't order the
        //same way every time - Kyle Hughart 7/20/17
        Collections.shuffle( drives );
        drives.sort( new TapeDriveExecutionPriorityComparator() );
        return drives;
    }


    private boolean stillActive( final TapeTask t )
    {
        Validations.verifyNotNull( "Task", t );
        synchronized ( m_taskStateLock )
        {
            if ( m_tapeTasks.getAllTapeTasks().contains( t ) )
            {
                return true;
            }
        }
        return false;
    }
    
    
    private void deleteTask( final TapeTask t )
    {
        Validations.verifyNotNull( "Task", t );
        synchronized ( m_taskStateLock )
        {
            if ( m_tapeTasks.remove( t, "deletion requested") )
            {
                if ( ChunkReadingTask.class.isAssignableFrom( t.getClass() ) )
                {
                    prepareReadTaskForRechunking( (ChunkReadingTask)t );
                }
                return;
            }
        }
        
        LOG.warn( "Could not find task to delete it: " + t );
    }
    
    
    private void prepareReadTaskForRechunking(final ChunkReadingTask task )
    {
    	final JobEntryService service = m_serviceManager.getService( JobEntryService.class );
        final Set<JobEntry> chunks =
        		service.retrieveAll( BeanUtils.toMap( task.getEntries() ).keySet() ).toSet();
		for ( final JobEntry chunk : chunks )
		{
            if ( null != chunk && JobChunkBlobStoreState.COMPLETED != chunk.getBlobStoreState() )
            {
                final Set< String > beanProperties = BeanUtils.getPropertyNames( ReadFromObservable.class );
                for ( final String bp : beanProperties )
                {
                    try
                    {
                        BeanUtils.getWriter( ReadFromObservable.class, bp ).invoke( 
                                chunk, new Object [] { null } );
                    }
                    catch ( final Exception ex )
                    {
                        throw new RuntimeException( ex );
                    }
                }
                service.update( chunk, CollectionFactory.toArray( String.class, beanProperties ) );
                LOG.warn( "Marked chunk " + chunk.getId() + " as requiring rechunking." );
            }
		}
    }
    
    
    private enum StartTaskResult
    {
        FAILED,
        STARTED_WITHOUT_TAPE_MOVE,
        STARTED_WITH_TAPE_MOVE
    }
    
    
    private StartTaskResult startTask( 
            final TapeTask t,
            final UUID tapeDriveId, 
            final TapeAvailability tapeAvailability )
    {
        final TapeDrive tapeDrive = m_serviceManager.getRetriever( TapeDrive.class ).attain( tapeDriveId );
        final TapeDriveResource tapeDriveResource;
        try
        {
            tapeDriveResource = m_tapeLockSupport.lock( tapeDriveId, t );
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to lock tape drive " + tapeDriveId + ".", ex );
            return StartTaskResult.FAILED;
        }
        
        try
        {
            try
            {
                t.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );
            }
            catch ( final RuntimeException ex )
            {
                LOG.error( "Failed to prepare to start.", ex );
                m_tapeLockSupport.unlock( t );
                return StartTaskResult.FAILED;
            }
            if ( null != t.getTapeId() )
            {
                m_tapeLockSupport.addTapeLock( t, t.getTapeId() );
            }
            if ( null == t.getTapeId() )
            {
                m_tapeLockSupport.unlock( t );
                LOG.info( "Cannot execute task since task did not specify a tape it could work with: " + t );
                return StartTaskResult.FAILED;
            }
            t.addSchedulingListener( m_tapeTaskStarter );
    
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( t.getTapeId() );
            final boolean tapeAlreadyInDrive = t.getTapeId().equals( tapeDrive.getTapeId() );
            final TapePartition partition = 
                    m_serviceManager.getRetriever( TapePartition.class ).attain( tapeDrive.getPartitionId() );
            LOG.info( Platform.NEWLINE + LogUtil.getLogMessageHeaderBlock( "Execute " + t )
                      + Platform.NEWLINE + "        Tape: " + t.getTapeId() 
                                                            + " (" + tape.getBarCode() + ")"
                      + Platform.NEWLINE + "  Tape Drive: " + tapeDriveId 
                                                            + " (" + tapeDrive.getSerialNumber() + ")"
                      + Platform.NEWLINE + "   Partition: " + tapeDrive.getPartitionId() 
                                                            + " (" + partition.getName() + ")"
                      + Platform.NEWLINE + "    Priority: " + t.getPriority()
                      + Platform.NEWLINE + "  Tape Moves: " 
                      + ( ( tapeAlreadyInDrive ) ? "0" : ( ( null == tapeDrive.getTapeId() ) ? "1" : "2" ) )
                      + Platform.NEWLINE );
            m_taskWorkPool.submit( ( tapeAlreadyInDrive ) ? 
                    t 
                    : new TapeChangingTaskExecutor( t, tapeDriveId ) );
            return ( tapeAlreadyInDrive ) ? 
                    StartTaskResult.STARTED_WITHOUT_TAPE_MOVE 
                    : StartTaskResult.STARTED_WITH_TAPE_MOVE;
        }
        catch ( final RuntimeException ex )
        {
            m_tapeLockSupport.unlock( t );
            LOG.warn( "Task invalid: " + t, ex );
            deleteTask( t );
            return StartTaskResult.FAILED;
        }
    }
    
    
    private final class TapeChangingTaskExecutor implements Runnable
    {
        private TapeChangingTaskExecutor( final TapeTask task, final UUID driveId )
        {
            m_task = task;
            m_driveId = driveId;
        }
        
        public void run()
        {
            Thread.currentThread().setName(
                    "ChangeTape-" + m_task.getClass().getSimpleName() + "#" + m_task.getId() );
            final Tape tapeToLoad = m_serviceManager.getRetriever( Tape.class ).attain( 
                    m_task.getTapeId() );
            final TapePartition partition = m_serviceManager.getRetriever( TapePartition.class ).attain( 
                    tapeToLoad.getPartitionId() );
            m_tapeEnvironment.lockPartition( partition.getSerialNumber() );
            try
            {
                if ( !m_tapeEnvironment.ensurePhysicalTapeEnvironmentUpToDate( Refresh.THROTTLE_CHECK_GEN_NUM ) )
                {
                    return;
                }
                
                final TapeDrive destTapeDrive = m_serviceManager.getRetriever( TapeDrive.class ).attain( 
                        m_driveId );
                if ( null != destTapeDrive.getTapeId() 
                        && !destTapeDrive.getTapeId().equals( tapeToLoad.getId() ) )
                {
                    LOG.info( "Must remove tape " + destTapeDrive.getTapeId() 
                              + " from the drive before we can move tape " + tapeToLoad.getId() 
                              + " into the drive." );
                    m_tapeEnvironment.performMove(
                            m_task.getDriveResource(),
                            partition.getSerialNumber(),
                            m_serviceManager.getRetriever( Tape.class ).attain( destTapeDrive.getTapeId() ),
                            new UnloadTapeFromDriveTapeMoveStrategy( 
                                    destTapeDrive, ElementAddressType.STORAGE ),
                            true );
                }
                
                final TapeMoveStrategy tapeMoveStrategy;
                final TapeDrive srcTapeDrive = m_serviceManager.getRetriever( TapeDrive.class ).retrieve(
                        TapeDrive.TAPE_ID, tapeToLoad.getId() );
                if ( null == srcTapeDrive || srcTapeDrive.getId().equals( destTapeDrive.getId() ) )
                {
                    tapeMoveStrategy = new LoadTapeIntoDriveTapeMoveStrategy( destTapeDrive );
                }
                else
                {
                    LOG.warn( "Drive-to-drive tape move required from " + srcTapeDrive.getId() + " to " 
                              + destTapeDrive.getId() 
                              + ".  Will force it from its current drive first to a storage slot first." );
                    handleTapeDriveRequiringTapeRemoval( srcTapeDrive, true );
                    tapeMoveStrategy =
                            new LoadTapeIntoDriveTapeMoveStrategy( destTapeDrive );
                }
                
                m_tapeEnvironment.performMove(
                        m_task.getDriveResource(),
                        partition.getSerialNumber(),
                        tapeToLoad, 
                        tapeMoveStrategy,
                        true );
            }
            catch ( final RuntimeException ex )
            {
                m_task.executionFailed( ex );
                m_tapeLockSupport.unlock( m_task );
                throw ex;
            }
            finally
            {
                m_tapeEnvironment.unlockPartition(partition.getSerialNumber());
                m_tapeTaskStarter.taskSchedulingRequired( m_task );
            }
            
            m_task.run();
        }
        
        private final TapeTask m_task;
        private final UUID m_driveId;
    } // end inner class def
    
    
    private List< TapeTask > getNextTasks(
            final BlobStoreTaskPriority minPriority,
            final TapeAvailability tapeAvailability,
            final boolean tapeMoveAllowed)
    {
        cleanUpIoTasksThatNoLongerApply();
        final HashMap<String, Integer> filterReasons = new HashMap<>();
        final Predicate<TapeTask> taskFilter = (TapeTask task) -> {
            //TODO: would be good to move handling of min drive priority, read/write drive/partition reservations into this filter
            //NOTE: we could use plain booleans here, but we use suppliers so we can evaluate lazily and avoid unnecessary work
            final Supplier<Boolean> tapeUnavailable =
                    () -> {
                        final boolean retval = !task.canUseAvailableTape(tapeAvailability);
                        if (retval) tallyFilterReason(filterReasons, UNAVAILABLE_REASON);
                        return retval;
                    };
            final Supplier<Boolean> moveRequiredButProhibited =
                    () -> {
                        final boolean retval = !tapeMoveAllowed && !task.canUseTapeAlreadyInDrive(tapeAvailability);
                        if (retval) tallyFilterReason(filterReasons, "tape move required but prohibited");
                        return retval;
                    };
            final Supplier<Boolean> canUseDrive =
                    () -> {
                        final boolean retval = task.canUseDrive(tapeAvailability.getDriveId());
                        if (!retval) tallyFilterReason(filterReasons, "task cannot use this drive");
                        return retval;
                    };
            return task.getPriority().isHigherOrEqualPriorityTo(minPriority) &&
                    !moveRequiredButProhibited.get() &&
                    !tapeUnavailable.get() &&
                    canUseDrive.get();
        };
        final UUID tapeIdToUse = tapeMoveAllowed ? null : tapeAvailability.getTapeInDrive();
        for (final IODirective directive : TapeWorkAggregationUtils.discoverTapeWorkAggregated(m_serviceManager, minPriority, tapeAvailability.getTapePartitionId(), tapeIdToUse)) {
            if (directive instanceof LocalWriteDirective wd) {
                final WriteChunkTapeSelectionStrategy tapeSelectionStrategy =
                        new WriteChunkTapeSelectionStrategy(m_serviceManager);
                final UUID selectedTape = tapeSelectionStrategy.selectTape(
                        wd.getSizeInBytes(),
                        wd.getStorageDomain().getId(),
                        wd.getBucket().getId(),
                        tapeAvailability,
                        false);
                if (selectedTape == null) {
                    tallyFilterReason(filterReasons, "could not select a tape for write");
                    continue;
                }
                write(wd);
            } else if (directive instanceof ReadIntoCacheDirective) {
                read((ReadIntoCacheDirective)directive);
            } else if (directive instanceof VerifyDirective) {
                verify((VerifyDirective)directive);
            } else {
                throw new IllegalStateException("Unknown directive type: " + directive);
            }
        }
        final List<TapeTask> retval = m_tapeTasks.getSortedPendingTapeTasksFor(new TapeTaskExecutionPriorityComparator( tapeAvailability ), taskFilter);
        if (!filterReasons.isEmpty()) {
            LOG.info("Some tasks will not be considered for execution on this drive: " + getFilterReport(filterReasons));
            if (filterReasons.containsKey(UNAVAILABLE_REASON)) {
                LOG.info("Tape availability summary: " + tapeAvailability.getSummary());
            }
        }
        return retval;
    }

    private void verify(VerifyDirective rd) {
        WorkAggregationUtils.markReadChunksInProgress(rd.getEntries(), m_serviceManager);
        m_tapeTasks.addStaticTask(new VerifyChunkOnTapeTask(
                rd,
                m_diskManager,
                m_jobProgressManager,
                m_tapeFailureManagement,
                m_serviceManager
        ));
    }

    private void read(ReadIntoCacheDirective rd) {
        WorkAggregationUtils.markReadChunksInProgress(rd.getEntries(), m_serviceManager);
        m_tapeTasks.addStaticTask(new ReadChunkFromTapeTask(
                rd,
                m_diskManager,
                m_jobProgressManager,
                m_tapeFailureManagement,
                m_serviceManager ));
    }

    private void write(LocalWriteDirective wd) {
        WorkAggregationUtils.markWriteChunksInProgress(wd, m_serviceManager);
        WorkAggregationUtils.markLocalDestinationsInProgress(wd.getDestinations(), m_serviceManager);
        m_tapeTasks.addDynamicTask(new WriteChunkToTapeTask(
                wd,
                m_tapeEjector,
                m_diskManager,
                m_jobProgressManager,
                m_tapeFailureManagement,
                m_serviceManager ));
    }


    private void tallyFilterReason(final HashMap<String, Integer> aggregator, final String reason) {
        aggregator.computeIfPresent(reason, (k,v) -> v + 1);
        aggregator.putIfAbsent(reason, 1);
    }


    private String getFilterReport(final HashMap<String, Integer> aggregator) {
        return aggregator.entrySet().stream().map((entry) -> {
                    return entry.getKey() + "(" + entry.getValue() + ")";
                }
        ).collect(Collectors.joining(" "));
    }

    private void cleanUpIoTasksThatNoLongerApply()
    {
        final Set< UUID > deletedChunkIds = m_tapeTasks.getChunkIds();
        deletedChunkIds.removeAll( BeanUtils.toMap( 
                m_serviceManager.getRetriever( JobEntry.class ).retrieveAll(
                        deletedChunkIds ).toSet() ).keySet() );
        if ( deletedChunkIds.isEmpty() )
        {
            return;
        }
        
        LOG.info( "I/O tasks have become invalid due to their chunks being deleted: " 
                  + LogUtil.getShortVersion( deletedChunkIds, 5 ) );
        for ( final UUID deletedChunkId : deletedChunkIds )
        {
            try
            {
                for (final IoTask t : m_tapeTasks.getTasksForChunk(deletedChunkId)) {
                    if (t.getState() != BlobStoreTaskState.COMPLETED) {
                        LOG.warn("Job chunk " + deletedChunkId + " no longer exists.");
                        deleteTask(t);
                    }
                }
            }
            catch ( final IllegalStateException ex )
            {
                LOG.info( "Cannot delete I/O task for job chunk " + deletedChunkId + " at this time: " 
                          + ex.getMessage() );
            }
        }
    }
    
    
    public TapeTaskQueueImpl getTapeTasks()
    {
        return m_tapeTasks;
    }


    public Object getTaskStateLock()
    {
        return m_taskStateLock;
    }
    
    
    private TapeAvailability getTapeAvailability(final TapeDrive tapeDrive, final boolean allowTapeMove)
    {
        final UUID partitionId = tapeDrive.getPartitionId();
        final Set< UUID > tapesInUnavailableState =
                BeanUtils.toMap( m_serviceManager.getRetriever( Tape.class ).retrieveAll(
                    Require.beanPropertyEqualsOneOf(
                            Tape.STATE,
                            TapeState.getStatesThatDisallowTapeLoadIntoDrive() ) ).toSet() ).keySet();
                            
        final Set< TapeDrive > drivesInError = m_serviceManager.getService( TapeDriveService.class ).retrieveAll(
        			Require.any(
        					Require.not( Require.beanPropertyEquals( TapeDrive.ERROR_MESSAGE , null ) ),
        					Require.beanPropertyEquals( TapeDrive.FORCE_TAPE_REMOVAL , Boolean.TRUE ) ) ).toSet();
        					
		final Set < UUID > tapesInDrivesInError = BeanUtils.extractPropertyValues( drivesInError , TapeDrive.TAPE_ID );
		tapesInDrivesInError.addAll( m_tapeEnvironment.getTapesInOfflineDrives() );
        tapesInDrivesInError.remove(null);

        return new TapeAvailabilityImpl(
                tapeDrive,
                m_tapeEnvironment.getTapesInPartition( partitionId ),
                m_tapeLockSupport,
                m_tapeEnvironment.getTapesNotInPartition( partitionId ),
                tapesInUnavailableState,
                tapesInDrivesInError,
                m_tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(tapeDrive.getId()),
                !allowTapeMove );
    }    
    
    
    public void addTaskSchedulingListener( final BlobStoreTaskSchedulingListener listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        m_taskSchedulingListeners.add( listener );
    }


    private final class TapeTaskDequeuedListenerImpl implements TapeTaskDequeuedListener
    {

        public void taskDequeued( final TapeTask task )
        {
            try
            {
                TapeBlobStoreProcessorImpl.this.m_tapeLockSupport.unlock( task );
                TapeBlobStoreProcessorImpl.this.taskSchedulingRequired();
            }
            catch ( final IllegalStateException ex )
            {
                LOG.debug( "Task did not hold any locks to release.", ex );
            }

            task.dequeued();
        }
    }

    
    public void taskSchedulingRequired()
    {
    	m_tapeTaskStarter.taskSchedulingRequired( null );
    }

    @Override
    public void setTapeEjector(TapeEjector ejector) {
        m_tapeEjector = ejector;
    }

    private final Object m_taskStateLock = new Object();
    private final TapeLockSupport< Object > m_tapeLockSupport;
    private TapeEjector m_tapeEjector;

    private final TapeTaskQueueImpl m_tapeTasks;
    private final Map<UUID, List<StaticTapeTask>> tasksForSpecificTapes = new HashMap<>();
    private final Object m_startTasksLock = new Object();
    private final BeansServiceManager m_serviceManager;
    private final TapeEnvironment m_tapeEnvironment;
    private final OfflineDurationTracker m_offlinePartitionTracker = new OfflineDurationTracker();
    private final List< BlobStoreTaskSchedulingListener > m_taskSchedulingListeners = 
            new CopyOnWriteArrayList<>();
    private final WorkPool m_taskWorkPool = 
            WorkPoolFactory.createWorkPool( 64, "TapeTaskExecutor" );
    private final TapeTaskStarter m_tapeTaskStarter;
    private final RecurringRunnableExecutor m_oldTapeFailuresDeleterExecutor =
            new RecurringRunnableExecutor( new OldTapeFailuresDeleter(), 6000000 );
    private final RecurringRunnableExecutor m_periodicTaskSchedulingExecutor;
    private final DiskManager m_diskManager;
    private final JobProgressManager m_jobProgressManager;
    protected final TapeFailureManagement m_tapeFailureManagement;

    private final static String UNAVAILABLE_REASON = "tape or tapes unavailable";
    private final static long MAX_TASK_SIZE_IN_BYTES = 30 * 1024 * 1024 * 1024L;
    private final static int MAX_ENTRIES_PER_TASK = 10000;
    private final static Logger LOG = Logger.getLogger( TapeBlobStoreProcessor.class );
}
