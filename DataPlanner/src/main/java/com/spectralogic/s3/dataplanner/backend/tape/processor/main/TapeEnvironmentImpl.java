package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.orm.TapePartitionRM;
import com.spectralogic.s3.common.dao.service.tape.*;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.TapePartitionResource;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.ReconcilingTapeEnvironmentManager;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.Refresh;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeMoveStrategy;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy.SlotToSlotTapeMoveStrategy;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy.UnloadTapeFromDriveTapeMoveStrategy;
import com.spectralogic.s3.dataplanner.backend.tape.task.NoOpTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservable;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TapeEnvironmentImpl implements TapeEnvironment {

    public TapeEnvironmentImpl(final ReconcilingTapeEnvironmentManager tapeEnvironmentManager,
                               final TapeEnvironmentResource tapeEnvironmentResource,
                               final BeansServiceManager serviceManager,
                               final TapeLockSupport<Object> tapeLockSupport,
                               final StaticCache<String, TapePartitionResource> tapePartitionResourceProvider,
                               final TapeFailureManagement tapeFailureManagement) {
        m_tapeEnvironmentManager = tapeEnvironmentManager;
        m_tapeEnvironmentResource = tapeEnvironmentResource;
        m_serviceManager = serviceManager;
        m_tapeLockSupport = tapeLockSupport;
        m_tapePartitionResourceProvider = tapePartitionResourceProvider;
        m_tapeFailureManagement = tapeFailureManagement;
    }

    private final class TapeMover implements Runnable
    {
        private TapeMover(
                final Tape tape,
                final TapeDrive drive,
                final ElementAddressType destinationSlotType,
                final TapeTask tapeLockHolder,
                final TapeDriveResource tapeDriveResource )
        {
            m_tape = tape;
            m_drive = drive;
            m_destinationSlotType = destinationSlotType;
            m_tapeLockHolder = tapeLockHolder;
            m_tapeDriveResource = tapeDriveResource;

            if ( null == m_drive )
            {
                m_moveStrategy = new SlotToSlotTapeMoveStrategy( m_destinationSlotType );
            }
            else
            {
                m_moveStrategy = new UnloadTapeFromDriveTapeMoveStrategy( m_drive, m_destinationSlotType );
            }
        }


        public void run()
        {
            try
            {
                runInternal();
            }
            finally
            {
                m_tapeLockSupport.unlock( m_tapeLockHolder );
            }
        }


        private void runInternal()
        {
            final TapePartition partition =
                    m_serviceManager.getRetriever( TapePartition.class ).attain( m_tape.getPartitionId() );
            final TapePartitionResource tpResource =
                    m_tapePartitionResourceProvider.get( partition.getSerialNumber() );

            if ( !m_partitionLockSupport.tryLock( tpResource ) )
            {
                LOG.info( "Cannot acquire a lock on partition " + partition.getId()
                        + " to move tape " + m_tape.getId() + " at this time." );
                m_partitionLockAcquired.add( Boolean.FALSE );
                return;
            }

            try
            {
                m_partitionLockAcquired.add( Boolean.TRUE );
                performMove( m_tapeDriveResource, tpResource, m_tape, m_moveStrategy, true );
            }
            catch ( final FailureTypeObservableException ex )
            {
                final int code = ex.getFailureType().getHttpResponseCode();
                if ( GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT.getHttpResponseCode() == code
                        || GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT.getHttpResponseCode() == code )
                {
                    LOG.info( "Cannot perform move at this time due to failure: "
                            + ExceptionUtil.getReadableMessage( ex ) );
                    return;
                }
                throw ex;
            }
            finally
            {
                m_partitionLockSupport.unlock( tpResource );
            }
        }


        private final Tape m_tape;
        private final TapeDrive m_drive;
        private final ElementAddressType m_destinationSlotType;
        private final TapeTask m_tapeLockHolder;
        private final TapeDriveResource m_tapeDriveResource;
        private final TapeMoveStrategy m_moveStrategy;
        private final BlockingQueue< Boolean > m_partitionLockAcquired = new ArrayBlockingQueue<>( 1 );
    } // end inner class def


    public void performMove(
            final TapeDriveResource tapeDriveResource,
            final String partitionSerial,
            final Tape tape,
            final TapeMoveStrategy moveStrategy,
            final boolean requireSuccessfulPrepareForRemovalToProceedWithMove )
    {
        final TapePartitionResource tpResource =
                m_tapePartitionResourceProvider.get( partitionSerial );
        performMove(tapeDriveResource, tpResource, tape, moveStrategy, requireSuccessfulPrepareForRemovalToProceedWithMove);
    }


    private void performMove(
            final TapeDriveResource tapeDriveResource,
            final TapePartitionResource tpResource,
            final Tape tape,
            final TapeMoveStrategy moveStrategy,
            final boolean requireSuccessfulPrepareForRemovalToProceedWithMove )
    {
        final TapeDrive driveContainingTape =
                m_serviceManager.getRetriever( TapeDrive.class ).retrieve( TapeDrive.TAPE_ID, tape.getId() );
        boolean failedPrepareForRemoval = false;
        if ( null != driveContainingTape )
        {
            try
            {
                tapeDriveResource.prepareForRemoval().get( RpcFuture.Timeout.LONG );
            }
            catch ( final RuntimeException ex )
            {
                final RuntimeException detailedException = new RuntimeException( "Failed to prepare tape for removal "
                        + tape.getId() + " (" + tape.getBarCode() + ") out of drive " + driveContainingTape.getId()
                        + " (" + driveContainingTape.getSerialNumber() + ")" + ".", ex );

                if ( requireSuccessfulPrepareForRemovalToProceedWithMove )
                {
                    m_moveFailureSupport.moveFailureOccurred( tape.getPartitionId() );
                    m_serviceManager.getService( TapePartitionFailureService.class ).create(
                            tape.getPartitionId(),
                            TapePartitionFailureType.MOVE_FAILED_DUE_TO_PREPARE_TAPE_FOR_REMOVAL_FAILURE,
                            detailedException,
                            null );
                    moveStrategy.moveFailed( tape );
                    for (final TapeDrive drive : moveStrategy.getAssociatedDrives()) {
                        m_tapeFailureManagement.registerFailure(tape.getId(), drive.getId(), TapeFailureType.MOVE_FAILED, ex);
                    }
                    throw detailedException;
                }
                failedPrepareForRemoval = true;
                LOG.warn( "Prepare for removal failed.  Will attempt move anyway.", detailedException );
            }
        }

        m_partitionLockSupport.lock( tpResource );
        m_tapeEnvironmentStateLock.readLock().lock();
        try
        {
            if ( m_moveFailureSupport.isTaskExecutionSuspended( tape.getPartitionId() ) )
            {
                flagForRefresh();
                moveStrategy.moveFailed( tape );
                throw new RuntimeException(
                        "Moves are suspended on partition " + tape.getPartitionId()
                                + " due to recent move failures." );
            }
            if ( failedPrepareForRemoval )
            {
                m_moveFailureSupport.moveFailureOccurred( tape.getPartitionId() );
            }

            boolean moveStarted = false;
            try
            {
                final int src = getTapeElementAddress( tape.getId() );
                final int dest = moveStrategy.getDest( src, tape, this);

                try
                {
                    moveStarted = true;
                    final RpcFuture< ? > moveFuture = tpResource.move( src, dest );
                    moveFuture.get( RpcFuture.Timeout.LONG );
                    moveStrategy.moveSucceeded();
                    m_moveFailureSupport.clearMoveFailure( tape.getPartitionId() );
                    for (final TapeDrive drive : moveStrategy.getAssociatedDrives()) {
                        m_tapeFailureManagement.resetFailures(tape.getId(), drive.getId(), TapeFailureType.MOVE_FAILED);
                    }
                }
                catch ( final RuntimeException ex )
                {
                    final String driveDescription = ( null == driveContainingTape ) ?
                            ""
                            : "out of drive " + driveContainingTape.getId()
                            + " (" + driveContainingTape.getSerialNumber() + ")";
                    throw new RuntimeException(
                            "Failed to move tape " + tape.getId() + " (" + tape.getBarCode() + ") "
                                    + driveDescription
                                    + " from " + src + " to " + dest + ".", ex );
                }
            }
            catch ( final RuntimeException ex )
            {
                if ( moveStarted )
                {
                    flagForRefresh();
                    m_moveFailureSupport.moveFailureOccurred( tape.getPartitionId() );
                    moveStrategy.moveFailed( tape );
                }
                if ( FailureTypeObservable.class.isAssignableFrom( ex.getClass() ) )
                {
                    final int code = ( (FailureTypeObservable)ex ).getFailureType().getHttpResponseCode();
                    if ( GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT.getHttpResponseCode() != code
                            && GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT.getHttpResponseCode() != code )
                    {
                        createMoveFailedPartitionFailure( tape.getPartitionId(), ex );
                        for (final TapeDrive drive : moveStrategy.getAssociatedDrives()) {
                            m_tapeFailureManagement.registerFailure(tape.getId(), drive.getId(), TapeFailureType.MOVE_FAILED, ex);
                        }
                    }
                }
                else
                {
                    createMoveFailedPartitionFailure( tape.getPartitionId(), ex );
                    for (final TapeDrive drive : moveStrategy.getAssociatedDrives()) {
                        m_tapeFailureManagement.registerFailure(tape.getId(), drive.getId(), TapeFailureType.MOVE_FAILED, ex);
                    }
                }
                throw ex;
            }
        }
        finally
        {
            m_partitionLockSupport.unlock( tpResource );
            m_tapeEnvironmentStateLock.readLock().unlock();
            taskSchedulingRequired();
        }
    }


    /**
     * Sends a tape move to a slot immediately to the underlying tape hardware.  This request will fail if
     * the tape in question is locked by another currently-running task.  This request will not block for the
     * actual move operation.  <br><br>
     *
     * @return TRUE if the request was accepted for validation and processing (although validation and
     * processing have not been started yet), or FALSE if the request was rejected and should be retried later
     */
    public boolean moveTapeToSlot(
            final UUID tapeId,
            final ElementAddressType destinationSlotType,
            final TapeMoveListener listener )
    {
        if ( !ensurePhysicalTapeEnvironmentUpToDate( Refresh.THROTTLE_CHECK_GEN_NUM ) )
        {
            return false;
        }

        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        final TapeDrive drive =
                m_serviceManager.getRetriever( TapeDrive.class ).retrieve( TapeDrive.TAPE_ID, tapeId );
        final TapeTask tapeLockHolder =
                new NoOpTapeTask( "Move Tape", BlobStoreTaskPriority.CRITICAL, tapeId, m_tapeFailureManagement, m_serviceManager );
        final TapeDriveResource tdResource;
        try
        {
            if ( null == drive )
            {
                tdResource = null;
                m_tapeLockSupport.lockWithoutDrive( tapeLockHolder );
            }
            else
            {
                tdResource = m_tapeLockSupport.lock( drive.getId(), tapeLockHolder );
            }
            if ( null != m_tapeLockSupport.getTapeLockHolder( tapeId ) )
            {
                m_tapeLockSupport.unlock( tapeLockHolder );
                return false;
            }
            m_tapeLockSupport.addTapeLock( tapeLockHolder, tapeId );
        }
        catch ( final IllegalStateException ex )
        {
            LOG.info( "Cannot lock needed resources at this time to perform tape move to slot.", ex );
            m_tapeLockSupport.unlock(tapeLockHolder);
            return false;
        }

        //The TapeMover's run method will release the lock from m_tapeLockSupport when complete
        final TapeMover tapeMover = new TapeMover(
                tape, drive, destinationSlotType, tapeLockHolder, tdResource );
        m_workPool.submit( tapeMover );
        try
        {
            if ( !tapeMover.m_partitionLockAcquired.take() )
            {
                return false;
            }
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        tapeMover.m_moveStrategy.addListener( listener );
        return true;
    }


    private void createMoveFailedPartitionFailure( final UUID partitionId, final RuntimeException ex )
    {
        m_serviceManager.getService( TapePartitionFailureService.class ).create(
                partitionId, TapePartitionFailureType.MOVE_FAILED, ex, 10 );
    }


    public Object getTapeLockHolder( final UUID tapeId )
    {
        return m_tapeLockSupport.getTapeLockHolder( tapeId );
    }


    public boolean tryPartitionLock(final String partitionSerial) {
        return m_partitionLockSupport.tryLock(m_tapePartitionResourceProvider.get( partitionSerial ));
    }


    public void lockPartition(final String partitionSerial) {
        m_partitionLockSupport.lock(m_tapePartitionResourceProvider.get( partitionSerial ));
    }


    public void unlockPartition(final String partitionSerial) {
        m_partitionLockSupport.unlock(m_tapePartitionResourceProvider.get( partitionSerial ));
    }

    public boolean threadsWaitingForPartitionLock(final String partitionSerial) {
        return m_partitionLockSupport.getNumberOfThreadsWaitingForLock(m_tapePartitionResourceProvider.get( partitionSerial )) != 0;
    }



    public boolean isTaskExecutionSuspended(final UUID partitionId) {
        return m_moveFailureSupport.isTaskExecutionSuspended( partitionId );
    }


    @Override
    public boolean ensurePhysicalTapeEnvironmentUpToDate(final Refresh refresh)
    {
        if ( !m_tapeEnvironmentResource.isServiceable() )
        {
            LOG.warn( "Tape environment is not serviceable at this time." );
            return false;
        }

        if ( Refresh.THROTTLE_CHECK_GEN_NUM == refresh
                && 1 > m_durationSinceTapeEnvironmentLastRefreshed.getElapsedSeconds() )
        {
            LOG.info( "Throttled tape environment gen number check since current state is stale by "
                    + m_durationSinceTapeEnvironmentLastRefreshed + "." );
            return true;
        }
        final long tapeEnvironmentGenerationNumber = m_tapeEnvironmentResource.getTapeEnvironmentGenerationNumber()
                .get( RpcFuture.Timeout.DEFAULT );
        final WhereClause tapesWithChangedBarCodeFilter = Require.beanPropertyMatches(Tape.BAR_CODE, CHANGED_BAR_CODE_PREFIX + "%");
        final BeansRetriever<Tape> tapeRetriever = m_serviceManager.getRetriever(Tape.class);
        if ( -1 < m_tapeEnvironmentGenerationNumber
                && Refresh.FORCED != refresh
                && m_tapeEnvironmentGenerationNumber == tapeEnvironmentGenerationNumber
                && 90 > m_durationSinceTapeEnvironmentLastRefreshed.getElapsedMinutes() 
                && !tapeRetriever.any(tapesWithChangedBarCodeFilter))
        {
            return true;
        }

        m_tapeEnvironmentStateLock.writeLock().lock();
        try
        {
            if ( -1 == m_tapeEnvironmentGenerationNumber )
            {
                m_tapeEnvironmentResource.quiesceState().get( RpcFuture.Timeout.VERY_LONG );
            }

            m_tapeEnvironmentManager.reconcileWith(
                    m_tapeEnvironmentResource.getTapeEnvironment().get( RpcFuture.Timeout.DEFAULT ) );
            m_tapeEnvironmentGenerationNumber = tapeEnvironmentGenerationNumber;
            m_durationSinceTapeEnvironmentLastRefreshed.reset();
            taskSchedulingRequired();
            return true;
        }
        finally
        {
            m_tapeEnvironmentStateLock.writeLock().unlock();
        }
    }

    @Override
    public void flagForRefresh() {
        m_tapeEnvironmentGenerationNumber = 0;
        taskSchedulingRequired();
    }

    @Override
    public void flagForQuiesceStateAndRefresh() {
        m_tapeEnvironmentGenerationNumber = -1;
    }

    @Override
    public int getTapeElementAddress(UUID tapeId) {
        return m_tapeEnvironmentManager.getTapeElementAddress(tapeId);
    }

    @Override
    public int getDriveElementAddress(UUID driveId) {
        return m_tapeEnvironmentManager.getDriveElementAddress(driveId);
    }

    @Override
    public boolean isSlotAvailable(UUID partitionId, ElementAddressType slotType) {
        return m_tapeEnvironmentManager.isSlotAvailable(partitionId, slotType);
    }

    @Override
    public int moveTapeSlotToSlot(UUID tapeId, ElementAddressType destinationSlotType) {
        return m_tapeEnvironmentManager.moveTapeSlotToSlot(tapeId, destinationSlotType);
    }

    @Override
    public void moveTapeSlotToSlot(UUID tapeId, int dest) {
        m_tapeEnvironmentManager.moveTapeSlotToSlot(tapeId, dest);
    }

    @Override
    public void moveTapeToDrive(UUID tapeId, TapeDrive drive) {
        m_tapeEnvironmentManager.moveTapeToDrive(tapeId, drive);
    }

    @Override
    public int moveTapeFromDrive(TapeDrive drive, ElementAddressType destinationSlotType) {
        return m_tapeEnvironmentManager.moveTapeFromDrive(drive, destinationSlotType);
    }

    @Override
    public void moveTapeDriveToDrive(TapeDrive src, TapeDrive dest) {
        m_tapeEnvironmentManager.moveTapeDriveToDrive(src, dest);
    }

    @Override
    public Set<UUID> getTapesInPartition(UUID partitionId) {
        return m_tapeEnvironmentManager.getTapesInPartition(partitionId);
    }

    @Override
    public Set<UUID> getTapesNotInPartition(UUID partitionId) {
        return m_tapeEnvironmentManager.getTapesNotInPartition(partitionId);
    }

    @Override
    public Set<UUID> getDrivesRequiringCleaning() {
        return m_tapeEnvironmentManager.getDrivesRequiringCleaning();
    }

    @Override
    public Set<UUID> getTapesInOfflineDrives() {
        return m_tapeEnvironmentManager.getTapesInOfflineDrives();
    }


    public void deleteOfflineTapePartition( final UUID partitionId )
    {
        m_tapeEnvironmentStateLock.writeLock().lock();
        try
        {
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                deleteOfflineTapePartitionInternal( transaction, partitionId );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
        } finally {
            m_tapeEnvironmentStateLock.writeLock().unlock();
        }
    }


    public void deleteOfflineTapeDrive( final UUID tapeDriveId ) {
        m_tapeEnvironmentStateLock.writeLock().lock();
        try
        {
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                deleteOfflineTapeDriveInternal( transaction, tapeDriveId );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
        }
        finally {
            m_tapeEnvironmentStateLock.writeLock().unlock();
        }
    }


    public void deletePermanentlyLostTape( final UUID tapeId ) {
        m_tapeEnvironmentStateLock.writeLock().lock();
        try
        {
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                deletePermanentlyLostPersistenceTarget( transaction, tapeId );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
        } finally {
            m_tapeEnvironmentStateLock.writeLock().unlock();
        }
    }


    private void deleteOfflineTapePartitionInternal(
            final BeansServiceManager transaction,
            final UUID partitionId )
    {
        final TapePartition partition =
                transaction.getRetriever( TapePartition.class ).attain( partitionId );
        if ( TapePartitionState.OFFLINE != partition.getState() )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Tape partition " + partition
                            + " is not offline.  Only offline tape partitions can be deleted." );
        }

        final Set< UUID > tapesThatContainData = BeanUtils.toMap(
                transaction.getRetriever( Tape.class ).retrieveAll( Require.all(
                        Require.beanPropertyEquals( Tape.PARTITION_ID, partitionId ),
                        Require.exists( BlobTape.class, BlobTape.TAPE_ID, Require.nothing() ) ) ).toSet() )
                .keySet();
        transaction.getService( TapeService.class ).deassociateFromPartition( tapesThatContainData );
        transaction.getService( TapePartitionService.class ).delete( partitionId );
        if ( 0 == transaction.getRetriever( TapePartition.class ).getCount(
                TapePartition.LIBRARY_ID, partition.getLibraryId() ) )
        {
            transaction.getService( TapeLibraryService.class ).delete( partition.getLibraryId() );
        }
    }


    private void deleteOfflineTapeDriveInternal(
            final BeansServiceManager transaction,
            final UUID tapeDriveId )
    {
        final TapeDrive drive =
                transaction.getRetriever( TapeDrive.class ).attain( tapeDriveId );
        if ( TapeDriveState.OFFLINE != drive.getState() )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Tape drive " + drive
                            + " is not offline.  Only offline tape drives can be deleted." );
        }

        transaction.getService( TapeDriveService.class ).delete( tapeDriveId );
    }


    private void deletePermanentlyLostPersistenceTarget(
            final BeansServiceManager transaction,
            final UUID tapeId )
    {
        final Tape tape = transaction.getRetriever( Tape.class ).attain( tapeId );
        if ( TapeState.LOST != tape.getState() && TapeState.EJECTED != tape.getState() )
        {
            if (new TapePartitionRM(tape.getPartitionId(), m_serviceManager).getQuiesced() != Quiesced.YES) {
                throw new FailureTypeObservableException(
                        GenericFailure.CONFLICT,
                        "Only " + TapeState.LOST + " and " + TapeState.EJECTED
                                + " tapes can be permanently deleted from an unquiesced partition." );
            } else if (m_serviceManager.getRetriever(DataPathBackend.class).attain(Require.nothing()).isIomEnabled()) {
                throw new FailureTypeObservableException(
                        GenericFailure.CONFLICT,
                        "Only " + TapeState.LOST + " and " + TapeState.EJECTED
                                + " tapes can be permanently deleted while IOM is enabled." );
            }
        }

        // create a new connection for processing the lost blobs to prevent a deadlock from
        // occurring over the db connection held by the EnhancedIterable
        final BeansServiceManager subTransaction = m_serviceManager.startTransaction();
        try ( final EnhancedIterable< BlobTape > iterable =
                      transaction.getRetriever( BlobTape.class ).retrieveAll(
                              Require.beanPropertyEquals( BlobTape.TAPE_ID, tapeId ) ).toIterable() )
        {
            final Set< UUID > blobIds = new HashSet<>();
            for ( final BlobTape bt : iterable )
            {
                blobIds.add( bt.getBlobId() );
                if ( 10000 <= blobIds.size() )
                {
                    deletePermanentlyLostPersistenceTarget( subTransaction, tapeId, blobIds );
                    blobIds.clear();
                }
            }
            deletePermanentlyLostPersistenceTarget( subTransaction, tapeId, blobIds );
            subTransaction.commitTransaction();
        }
        finally
        {
            subTransaction.closeTransaction();

        }
        transaction.getService( TapeService.class ).delete( tapeId );
    }


    private void deletePermanentlyLostPersistenceTarget(
            final BeansServiceManager transaction,
            final UUID tapeId,
            final Set< UUID > blobIds )
    {
        transaction.getService( BlobTapeService.class ).blobsLost(
                "Tape has been marked as permanently lost.",
                tapeId,
                blobIds );
    }

    private void taskSchedulingRequired() {
        for ( final BlobStoreTaskSchedulingListener listener : m_schedulingListeners )
        {
            listener.taskSchedulingRequired( null );
        }
    }


    final public void addSchedulingListener( final BlobStoreTaskSchedulingListener listener )
    {
        if ( m_schedulingListeners.contains( listener ) )
        {
            return;
        }

        m_schedulingListeners.add( listener );
    }


    private final ReconcilingTapeEnvironmentManager m_tapeEnvironmentManager;
    private final Duration m_durationSinceTapeEnvironmentLastRefreshed = new Duration();
    private final TapeEnvironmentResource m_tapeEnvironmentResource;
    private final ReentrantReadWriteLock m_tapeEnvironmentStateLock = new ReentrantReadWriteLock( true );
    private long m_tapeEnvironmentGenerationNumber = -1;
    private final static Logger LOG = Logger.getLogger( TapeEnvironmentImpl.class );
    private final BeansServiceManager m_serviceManager;
    private final TapeLockSupport< Object > m_tapeLockSupport;
    private final TapePartitionLockSupport m_partitionLockSupport = new TapePartitionLockSupport();
    //NOTE: Passing 0 into TapePartitionMoveFailureSupport effectively disables this feature, but it is left in place
    //in case we want to change it in the future.
    private final TapePartitionMoveFailureSupport m_moveFailureSupport =
            new TapePartitionMoveFailureSupport( 0 );
    private final TapeFailureManagement m_tapeFailureManagement;
    private final StaticCache< String, TapePartitionResource > m_tapePartitionResourceProvider;
    private final WorkPool m_workPool =
            WorkPoolFactory.createWorkPool( 64, "TapeMoveManager" );
    private final List< BlobStoreTaskSchedulingListener > m_schedulingListeners =
            new CopyOnWriteArrayList<>();
}
