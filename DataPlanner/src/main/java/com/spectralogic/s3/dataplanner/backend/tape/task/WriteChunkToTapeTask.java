/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.nio.file.Paths;
import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.orm.BlobLocalDestinationRM;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.ds3.LocalBlobDestinationService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.ObsoletionService;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.iom.IomUtils;
import com.spectralogic.s3.common.platform.iom.PersistenceProfile;
import com.spectralogic.s3.common.platform.lang.NeedsImplementForRefactorException;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.BucketIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.dataplanner.backend.frmwrk.LocalWriteDirective;
import com.spectralogic.s3.dataplanner.backend.tape.api.DynamicTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureAction;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils.FailureHandling;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils.RestoreExpected;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.manager.DatabaseErrorCodes;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.client.RpcTimeoutException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.render.BytesRenderer;
import lombok.NonNull;

import static com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType.DOES_NOT_EXIST;

public final class WriteChunkToTapeTask extends BaseIoTask implements DynamicTapeTask
{

    public WriteChunkToTapeTask(
            final BlobStoreTaskPriority priority,
            final Set<LocalBlobDestination> destinations,
            final TapeEjector tapeEjector,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final TapeFailureManagement tapeFailureManagement,
            final BeansServiceManager beansServiceManager)
    {
        this(getLocalWriteDirective(priority, destinations, beansServiceManager), tapeEjector, diskManager, jobProgressManager, tapeFailureManagement, beansServiceManager);
    }

    private static LocalWriteDirective getLocalWriteDirective(BlobStoreTaskPriority priority, Set<LocalBlobDestination> destinations, BeansServiceManager serviceManager) {
        if (destinations.isEmpty()) {
            throw new IllegalArgumentException("Cannot create a write task with no destinations.");
        }
        final BlobLocalDestinationRM destRM = new BlobLocalDestinationRM(destinations.iterator().next(), serviceManager);
        final StorageDomain storageDomain = destRM.getStorageDomain().unwrap();
        final Bucket bucket = destRM.getJobEntry().getJob().getBucket().unwrap();
        final List<JobEntry> entries = serviceManager.getRetriever(JobEntry.class).retrieveAll(
                Require.beanPropertyEqualsOneOf(
                        Identifiable.ID,
                        BeanUtils.extractPropertyValues(
                                destinations,
                                LocalBlobDestination.ENTRY_ID))).toList();
        final Set<UUID> blobIds = BeanUtils.extractPropertyValues(entries, JobEntry.BLOB_ID);
        final long sizeInBytes = serviceManager.getRetriever(Blob.class).getSum(Blob.LENGTH, Require.beanPropertyEqualsOneOf(
                Identifiable.ID,
                blobIds));
        final LocalWriteDirective dir=  new LocalWriteDirective(
                destinations,
                storageDomain,
                priority,
                entries,
                sizeInBytes,
                bucket);
        return dir;
    }


    public WriteChunkToTapeTask(
            @NonNull final LocalWriteDirective writeDirective,
            final TapeEjector tapeEjector,
            @NonNull final DiskManager diskManager,
            @NonNull final JobProgressManager jobProgressManager,
            @NonNull final TapeFailureManagement tapeFailureManagement,
            @NonNull final BeansServiceManager beansServiceManager)
    {
        super( writeDirective.getPriority(),  null, diskManager, jobProgressManager, tapeFailureManagement, beansServiceManager );
        m_writeDirective = writeDirective;
        m_tapeEjector = tapeEjector;
    }


    @Override
    protected BlobStoreTaskState runInternal()
    {
        for ( final LocalBlobDestination chunk : m_writeDirective.getDestinations())
        {
            if ( null == getServiceManager().getRetriever( JobEntry.class ).retrieve( chunk.getEntryId() ) )
            {
                //NOTE: failing here will put the task back into a ready state so it can be safely re-aggregated or
                //invalidated by TapeBlobStoreProcessorImpl.cleanUpIoTasksThatNoLongerApply().
                throw new RuntimeException("Job chunk " + chunk.getId() + " no longer exists.");
            }
        }
    	
    	if ( m_writeDirective.getDestinations().isEmpty() )
		{
    		LOG.info( "No job chunks to write, no work to perform." );
    		return BlobStoreTaskState.COMPLETED;
		}
        
        final Tape tape = getTape();
        verifyTapeIsWritable();
        if ( TapeState.NORMAL != tape.getState() )
        {
            throw new IllegalStateException( 
                    "Tape " + tape + " is in state " + tape.getState() 
                    + ", which is illegal if we are to write a chunk to it." );
        }
        
        final Set<UUID> entryIds = BeanUtils.extractPropertyValues(m_writeDirective.getDestinations(), LocalBlobDestination.ENTRY_ID);
		LOG.info("Servicing " + m_writeDirective.getEntries().size() + " job entries from " + m_writeDirective.getDestinations().size() + " chunks in one task: ["
				+ entryIds + "].");
        final Map< UUID, DetailedJobEntry> jobEntriesToVerify = BeanUtils.toMap(
                getServiceManager().getRetriever( DetailedJobEntry.class ).retrieveAll( Require.all(
                        Require.beanPropertyEqualsOneOf(
                        		JobEntry.ID,
                        		BeanUtils.extractPropertyValues(m_writeDirective.getDestinations(), LocalBlobDestination.ENTRY_ID) ),
                		Require.exists(
                				JobEntry.JOB_ID,
                				Require.beanPropertyEquals(
                						Job.VERIFY_AFTER_WRITE,
                						true ) ) ) ).toSet() );
        final S3ObjectsIoRequest ioRequest =
                constructObjectsIoRequestFromJobEntries( JobRequestType.PUT, new HashSet<>( m_writeDirective.getEntries() ) );
        final StorageDomain storageDomain =
                new TapeRM( tape, m_serviceManager ).getStorageDomainMember().getStorageDomain().unwrap();
        updateTapeDateLastModified();
        try
        {
            final BytesRenderer bytesRenderer = new BytesRenderer();
            final Duration duration = new Duration();
            final String dataDescription =
                    m_writeDirective.getEntries().size() + " blobs (" + bytesRenderer.render( m_writeDirective.getSizeInBytes() ) + ")";
            LOG.info( "Will write " + dataDescription + "..." );
            final BlobIoFailures response = getDriveResource().writeData(
                    storageDomain.getLtfsFileNaming(),
                    ioRequest ).get( Timeout.VERY_LONG );
            LOG.info( dataDescription + " written to tape at " 
                      + bytesRenderer.render( m_writeDirective.getSizeInBytes(), duration ) + "." );
            for ( final BlobIoFailure failure : response.getFailures() )
            {
            	// If the failed blob still exists in our database
            	if ( null !=  m_serviceManager.getRetriever( Blob.class ).retrieve( failure.getBlobId() ) )
            	{
                    if (failure.getFailure() == DOES_NOT_EXIST) {
                        final DiskFileInfo fileOnDisk = m_diskManager.getDiskFileFor(failure.getBlobId());
                        if (fileOnDisk != null && fileOnDisk.getBlobPoolId() != null) { //disk file is on pool
                            m_serviceManager.getService(BlobPoolService.class).registerFailureToRead(fileOnDisk);
                            throw new WriteSourceFailedException(response.getFailures());
                        } else { //disk file is (or should be) on cache
                            throw new RuntimeException("Unable to read blob " + failure.getBlobId() + " from cache: " + failure.getFailure().toString());
                        }
                    } else {
                        throw new BlobFailuresOccurredException( response.getFailures() );
                    }
            	}
            }
            if ( 0 != response.getFailures().length )
            {
            	LOG.warn( "Some blobs were not written successfully, but all such blobs appear to have since been "
            			+ "deleted from the frontend database, so will not register a failure. ",
            			new BlobFailuresOccurredException( response.getFailures() ) );
            } 
            
            m_serviceManager.getService( TapeService.class ).update(
                    getTape().setAllowRollback(true),
                    Tape.ALLOW_ROLLBACK);
            final String checkpoint = getDriveResource().quiesce().get( Timeout.VERY_LONG );
            LOG.info( dataDescription + " quiesced to tape (effectively "
                      + bytesRenderer.render( m_writeDirective.getSizeInBytes(), duration ) + "." );
            
            if ( !jobEntriesToVerify.isEmpty() )
            {
            	final S3ObjectsIoRequest verifyIoRequest = constructObjectsIoRequestFromJobEntries(
            			JobRequestType.VERIFY, new HashSet<>( jobEntriesToVerify.values() ) );
                final Duration verifyDuration = new Duration();
                final BlobIoFailures failures = getDriveResource().verifyData(
                        TapeTaskUtils.buildVerifyObjectsPayload( verifyIoRequest ) ).get( Timeout.VERY_LONG );
                if ( 0 < failures.getFailures().length )
                {
                    LOG.warn( "Verification of data failed after it was written and successfully quiesced." );
                    throw new BlobFailuresOccurredException( failures.getFailures() );
                }
                final long totalBytesVerified = jobEntriesToVerify.values().stream().mapToLong( DetailedJobEntry::getLength ).sum();
                LOG.info( dataDescription + " verified on tape at "
                          + bytesRenderer.render( totalBytesVerified, verifyDuration ) + "." );
            }
            
            int orderIndex =
                    getServiceManager().getService( BlobTapeService.class ).getNextOrderIndex( getTapeId() );
            final Set< BlobTape > blobTapes = new HashSet<>();
            for ( final BucketIoRequest bucketIoRequest : ioRequest.getBuckets() )
            {
                for ( final S3ObjectIoRequest objectIoRequest : bucketIoRequest.getObjects() )
                {
                    for ( final BlobIoRequest blobIoRequest : objectIoRequest.getBlobs() )
                    {
                        final UUID id = UUID.fromString( Paths.get( blobIoRequest.getFileName() )
                                                              .getFileName()
                                                              .toString() );
                        blobTapes.add( BeanFactory.newBean( BlobTape.class )
                                .setBlobId( id )
                                .setTapeId( getTapeId() )
                                .setOrderIndex( orderIndex++ ) );
                    }
                }
            }
            final BeansServiceManager transaction = 
                    getServiceManager().startTransaction();
            try
            {
                transaction.getService( TapeService.class ).update(
                        getTape().setLastCheckpoint( checkpoint )
                                .setAllowRollback(false),
                        Tape.LAST_CHECKPOINT, Tape.ALLOW_ROLLBACK);
                
                Obsoletion obsoletion = null;
                final Set< UUID > blobIds = BeanUtils.extractPropertyValues( blobTapes, BlobObservable.BLOB_ID);
                final Set< BlobTape > blobTapesToObsolete = transaction.getRetriever( BlobTape.class ).retrieveAll(
                        Require.all(
                                IomUtils.blobTapePersistedToStorageDomain(
                                        storageDomain.getId(),
                                        //NOTE: do not re-obsolete blob records that are already obsolete
                                        PersistenceProfile.EVERYTHING_BUT_OBSOLETE ),
                                Require.beanPropertyEqualsOneOf(
                                        BlobObservable.BLOB_ID,
                                        blobIds ) ) ).toSet();
                final Set< BlobPool > blobPoolsToObsolete = transaction.getRetriever( BlobPool.class ).retrieveAll(
                        Require.all(
                                IomUtils.blobPoolPersistedToStorageDomain(
                                        storageDomain.getId(),
                                        //NOTE: do not re-obsolete blob records that are already obsolete
                                        PersistenceProfile.EVERYTHING_BUT_OBSOLETE ),
                                Require.beanPropertyEqualsOneOf(
                                        BlobObservable.BLOB_ID,
                                        blobIds ) ) ).toSet();
                if ( !blobTapesToObsolete.isEmpty() )
                {
                    LOG.warn( blobTapesToObsolete.size() + " blob tape records are now obsolete and will be kept track"
                            + " of to be deleted following the next database backup.");
                    obsoletion = BeanFactory.newBean( Obsoletion.class );
                    transaction.getService( ObsoletionService.class ).create( obsoletion );
                    transaction.getService( BlobTapeService.class ).obsoleteBlobTapes(
                        blobTapesToObsolete,
                        obsoletion.getId() );
                }
                if ( !blobPoolsToObsolete.isEmpty() )
                {
                    LOG.warn( blobPoolsToObsolete.size() + " blob pool records are now obsolete and will be kept track"
                            + " of to be deleted following the next database backup.");
                    obsoletion = BeanFactory.newBean( Obsoletion.class );
                    transaction.getService( ObsoletionService.class ).create( obsoletion );
                    transaction.getService( BlobPoolService.class ).obsoleteBlobPools(
                        blobPoolsToObsolete,
                        obsoletion.getId() );
                }
                transaction.getService( BlobTapeService.class ).create( blobTapes );
                final LocalBlobDestinationService service =
                        transaction.getService( LocalBlobDestinationService.class );
                final Set<UUID> ptIds = BeanUtils.toMap(m_writeDirective.getDestinations()).keySet();
                service.update(Require.beanPropertyEqualsOneOf(Identifiable.ID, ptIds),
                        (pt) -> pt.setBlobStoreState(JobChunkBlobStoreState.COMPLETED),
                        LocalBlobDestination.BLOB_STORE_STATE);

                transaction.commitTransaction();
                transaction.closeTransaction();
                if ( null != obsoletion )
                {
                    m_serviceManager.getService( ObsoletionService.class )
                        .update( obsoletion.setDate( new Date() ), Obsoletion.DATE );
                }
            }
            catch ( final DaoException ex )
            {
                transaction.closeTransaction();
                if ( DatabaseErrorCodes.isExceptionCausedByErrorCode( ex,
                        DatabaseErrorCodes.FOREIGN_KEY_VIOLATION ) )
                {
                    LOG.warn( "Cannot commit write results since blobs were deleted while "
                            + "I/O was in progress. Will rollback and retry.", ex );
                    transaction.closeTransaction();
                    TapeTaskUtils.verifyQuiescedToCheckpoint(
                            getTape(),
                            getDriveResource(),
                            getServiceManager(),
                            m_tapeFailureManagement,
                            RestoreExpected.YES,
                            FailureHandling.LOG_IT );
                    return BlobStoreTaskState.READY;
                }
                throw ex;
            }
            finally
            {
                transaction.closeTransaction();
            }
            
            updateTapeExtendedInformation();
            ejectTapeIfNecessary();
            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.WRITE_FAILED);
        }
        catch ( final RpcTimeoutException ex )
        {
            LOG.warn( "Failed to write chunk to tape.", ex );
            return BlobStoreTaskState.READY;
        }
        catch ( final WriteSourceFailedException ex )
        {
            LOG.warn( "Failed to write chunk to tape due to source read failure.", ex );
            handleWriteSourceFailure( ex );
            return BlobStoreTaskState.READY;
        }
        catch ( final RpcProxyException | BlobFailuresOccurredException ex )
        {
            LOG.warn( "Failed to write chunk to tape.", ex );
            handleFailure( ex );
            return BlobStoreTaskState.READY;
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to write chunk to tape.", ex );
            throw ex;
        }
        finally
        {
            updateTapeDateLastModified();
        }
        return BlobStoreTaskState.COMPLETED;
    }
    
    
    private void ejectTapeIfNecessary()
    {
        final Tape tape = getTape();
        final StorageDomain storageDomain =
                new TapeRM( tape, m_serviceManager ).getStorageDomainMember().getStorageDomain().unwrap();
        if ( !storageDomain.isAutoEjectUponMediaFull() )
        {
            return;
        }
        
        final long minimum;
        final long available = tape.getAvailableRawCapacity();
        if ( null == storageDomain.getAutoEjectMediaFullThreshold() )
        {
            //eject when tape is 95% full
            minimum = tape.getTotalRawCapacity() / 20;
        }
        else
        {
            minimum = storageDomain.getAutoEjectMediaFullThreshold();
        }
        
        final String action = ( available < minimum ) ? "will auto-eject" : "will not auto-eject yet";
        final BytesRenderer bytesRenderer = new BytesRenderer();
        LOG.info( "Tape has " + bytesRenderer.render( available ) 
                  + " available (auto-eject threshold is " + bytesRenderer.render( minimum ) 
                  + ", so " + action + ")." );
        if ( available < minimum )
        {
            if (m_tapeEjector == null) {
                throw new NeedsImplementForRefactorException("we aren't currently passing in the tape ejector");
            }
            m_tapeEjector.ejectTape( 
                    storageDomain.getVerifyPriorToAutoEject(),
                    tape.getId(), 
                    "Auto-exported by storage domain policy since tape had "
                    + bytesRenderer.render( available )
                    + " remaining (it needed to have at least " 
                    + bytesRenderer.render( minimum )
                    + " to not be auto-exported)",
                    null );
        }
    }

    @Override
    public Collection<UUID> getChunkIds() {
        return BeanUtils.extractPropertyValues(getPersistenceTargets(), LocalBlobDestination.ENTRY_ID);
    }


    private void handleWriteSourceFailure( final WriteSourceFailedException ex )
    {
        TapeTaskUtils.verifyQuiescedToCheckpoint(
                getTape(),
                getDriveResource(),
                getServiceManager(),
                m_tapeFailureManagement,
                RestoreExpected.YES,
                FailureHandling.LOG_IT );
        m_tapeFailureManagement.registerFailure(
                getTapeId(),
                TapeFailureType.WRITE_SOURCE_FAILED,
                ex );
    }


    private void handleFailure( final RuntimeException ex )
    {
        TapeTaskUtils.verifyQuiescedToCheckpoint(
                getTape(),
                getDriveResource(),
                getServiceManager(),
                m_tapeFailureManagement,
                RestoreExpected.YES,
                FailureHandling.LOG_IT );
        final TapeFailureAction action = m_tapeFailureManagement.registerFailure(
                getTapeId(),
                TapeFailureType.WRITE_FAILED,
                ex );
        if (action == TapeFailureAction.TAPE_MARKED_BAD) {
            m_tapesMarkedBad++;
        }
    }
    

    private String selectTape(
            final TapeAvailability tapeAvailability)
    {
            final WriteChunkTapeSelectionStrategy tapeSelectionStrategy =
                    new WriteChunkTapeSelectionStrategy(getServiceManager());
            final UUID selectedTape = tapeSelectionStrategy.selectTape(
                    m_writeDirective.getSizeInBytes(),
                    m_writeDirective.getStorageDomain().getId(),
                    m_writeDirective.getBucket().getId(),
                    tapeAvailability,
                    true
            );
            setTapeId(selectedTape);
            final String chunksDescription = "Chunks " + BeanUtils.toMap(m_writeDirective.getDestinations()).keySet();
            if (selectedTape == null) {
                return ("No tapes available that will fit " + chunksDescription + ", which needs "
                        + new BytesRenderer().render(m_writeDirective.getSizeInBytes()) + ".");
            }
        return null;
    }


    public boolean canUseAvailableTape(final TapeAvailability tapeAvailability) {
        try {
            final WriteChunkTapeSelectionStrategy tapeSelectionStrategy =
                    new WriteChunkTapeSelectionStrategy(getServiceManager());

            final UUID candidate = tapeSelectionStrategy.selectTape(
                    m_writeDirective.getSizeInBytes(),
                    m_writeDirective.getStorageDomain().getId(),
                    m_writeDirective.getBucket().getId(),
                    tapeAvailability,
                    false
            );

            return candidate != null;
        } catch ( final Exception e ) {
            LOG.warn("Failed to determine whether " + this
                    + " can use tape " + tapeAvailability.getTapeInDrive()
                    + " already in drive " + tapeAvailability.getDriveId() + ".", e);
            return false;
        }
    }

    public boolean canUseTapeAlreadyInDrive(final TapeAvailability tapeAvailability) {
        try {
            if (tapeAvailability.getTapeInDrive() == null) {
                return false;
            }

            final WriteChunkTapeSelectionStrategy tapeSelectionStrategy =
                    new WriteChunkTapeSelectionStrategy(getServiceManager());

            final UUID candidate = tapeSelectionStrategy.selectTape(
                    m_writeDirective.getSizeInBytes(),
                    m_writeDirective.getStorageDomain().getId(),
                    m_writeDirective.getBucket().getId(),
                    tapeAvailability,
                    false
            );

            return candidate != null && candidate.equals(tapeAvailability.getTapeInDrive());
        } catch ( final Exception e ) {
            LOG.warn("Failed to determine whether " + this
                    + " can use tape " + tapeAvailability.getTapeInDrive()
                    + " already in drive " + tapeAvailability.getDriveId() + ".", e);
            return false;
        }
    }


    public List<? extends LocalBlobDestination> getPersistenceTargets()
    {
        return m_writeDirective.getDestinations();
    }


    @Override
    public String getDescription()
    {
        return "Write " + m_writeDirective.getDestinations().size() + " Chunks";
    }


    private void setTapeId(final UUID tapeId) {
        m_tapeId = tapeId;
    }

    @Override
    public void prepareForExecutionIfPossible(
            final TapeDriveResource tapeDriveResource,
            final TapeAvailability tapeAvailability )
    {
        if (m_tapesMarkedBad >= MAX_TAPES_TASK_CAN_MARK_BAD) {
            invalidateTaskAndThrow(new RuntimeException("This task has marked too many tapes bad. Will invalidate it."));
        }
        String failureToSelectTape = null;
        try {
            failureToSelectTape = selectTape(tapeAvailability);
        } catch ( final RuntimeException e) {
            invalidateTaskAndThrow( e );
        }
        if (failureToSelectTape != null) {
            throw new RuntimeException(failureToSelectTape);
        }
        super.prepareForExecutionIfPossible(tapeDriveResource, tapeAvailability);
    }

    @Override
    public final UUID getTapeId()
    {
        return m_tapeId;
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_writeDirective.getEntries(), JobEntry.JOB_ID).toArray(new UUID[0]);
    }

	/* cached size; if this is zero it is not valid */
    private final LocalWriteDirective m_writeDirective;
    private final TapeEjector m_tapeEjector;
    private UUID m_tapeId = m_defaultTapeId;
    private int m_tapesMarkedBad = 0;

    private static final int MAX_TAPES_TASK_CAN_MARK_BAD = 3;
}
