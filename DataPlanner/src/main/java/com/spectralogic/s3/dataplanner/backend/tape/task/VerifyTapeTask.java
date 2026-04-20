/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeService.TapeAccessType;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeResourceFailureCode;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.dataplanner.backend.tape.api.LongRunningInterruptableTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Query.LimitableRetrievable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.render.BytesRenderer;

public final class VerifyTapeTask extends BaseBlobTask implements LongRunningInterruptableTapeTask, StaticTapeTask
{
    public VerifyTapeTask( final BlobStoreTaskPriority priority,
                           final UUID tapeId,
                           final DiskManager diskManager,
                           final TapeFailureManagement tapeFailureManagement,
                           final BeansServiceManager serviceManager )
    {
        this( priority, tapeId, 10000, diskManager, tapeFailureManagement, serviceManager );
    }


    // This constructor is used for testing when a larger max retries is needed in order for tests to run in a
    // timely fashion.
    protected VerifyTapeTask( final BlobStoreTaskPriority priority,
                              final UUID tapeId,
                              final DiskManager diskManager,
                              final TapeFailureManagement tapeFailureManagement,
                              final BeansServiceManager serviceManager,
                              final int maxRetriesBeforeSuspensionRequired )
    {
        super( priority, tapeId, diskManager, tapeFailureManagement, serviceManager, maxRetriesBeforeSuspensionRequired );
        m_maxSegmentCount = 10000;
    }

    VerifyTapeTask( final BlobStoreTaskPriority priority,
                    final UUID tapeId,
                    final int maxSegmentCount,
                    final DiskManager diskManager,
                    final TapeFailureManagement tapeFailureManagement,
                    final BeansServiceManager serviceManager )
    {
        super( priority, tapeId, diskManager, tapeFailureManagement, serviceManager );
        m_maxSegmentCount = maxSegmentCount;
    }
    
    
    public UUID getTapeToVerify()
    {
        return m_defaultTapeId;
    }


    @Override
    protected BlobStoreTaskState runInternal()
    {
        // Check to see if it's still valid to verify this tape
        final Tape tape = getTape();
        if ( null == tape.getStorageDomainMemberId() )
        {
            LOG.warn( "Tape not assigned to a storage domain, so nothing to verify." );
            getServiceManager().getService( TapeService.class ).update(
                    tape.setVerifyPending( null ),
                    Tape.VERIFY_PENDING );
            return BlobStoreTaskState.COMPLETED;
        }
        if ( TapeState.NORMAL != tape.getState() )
        {
            LOG.warn( "Cannot verify tape while it is in state " + tape.getState() + "." );
            return BlobStoreTaskState.COMPLETED;
        }
        final int blobsOnTape = getServiceManager().getRetriever( BlobTape.class )
                                                   .getCount( Query.where( Require.beanPropertyEquals( BlobTape.TAPE_ID, getTapeId() ) ) );
        if ( 0 == blobsOnTape )
        {
            LOG.warn( "Tape has no associated blobs, so nothing to verify." );
            getServiceManager().getService( TapeService.class )
                               .update( tape.setVerifyPending( null ), Tape.VERIFY_PENDING );
            return BlobStoreTaskState.COMPLETED;
        }
        if ( null == m_dataPathBackend )
        {
            m_dataPathBackend = 
                    getServiceManager().getRetriever( DataPathBackend.class ).attain( Require.nothing() );
        }
        
        if ( null == m_dataPathBackend.getPartiallyVerifyLastPercentOfTapes() )
        {
            return runFullVerify();
        }
        
        LOG.warn( "Full verifies are disabled.  Will only verify the last " 
                + m_dataPathBackend.getPartiallyVerifyLastPercentOfTapes() + "% of tape " 
                + tape.getId() + " (" + tape.getBarCode() + ")'s data." );
        return runPartialVerify( m_dataPathBackend.getPartiallyVerifyLastPercentOfTapes() );
    }
    
    
    private BlobStoreTaskState runFullVerify()
    {
        // Compute segement to verify
        final List< UUID > orderedBlobIds = new ArrayList<>();
        final Map< UUID, OrderedBlobEntry> jobEntries = createJobEntries( orderedBlobIds );
        if ( m_maxSegmentCount == jobEntries.size() )
        {
            LOG.info( "There are at least " + jobEntries.size() + " blobs remaining to be verified." );
        }
        else
        {
            LOG.info( "There are " + jobEntries.size() + " blobs remaining to be verified." );
        }
        
        // Cut down segment based on max segment bytes
        final long maxSegmentLength = getPageSegmentLength();
        LOG.info( "The max verification segment length is " 
                  + new BytesRenderer().render( maxSegmentLength ) + "." );
        long segmentBytes = 0;
        final Map< UUID, Blob > blobs = BeanUtils.toMap( 
                getServiceManager().getRetriever( Blob.class ).retrieveAll( jobEntries.keySet() ).toSet() );
        for ( final UUID blobId : orderedBlobIds )
        {
            final Blob blob = blobs.get( blobId );
            if ( 0 == segmentBytes || blob.getLength() + segmentBytes <= maxSegmentLength )
            {
                segmentBytes += blob.getLength();
            }
            else
            {
                jobEntries.remove( blob.getId() );
            }
        }
        
        // Check if we're done
        if ( jobEntries.isEmpty() )
        {
            getServiceManager().getService( TapeService.class ).updateDates(
                    getTapeId(), TapeAccessType.VERIFIED );
            return BlobStoreTaskState.COMPLETED;
        }
        
        // Verify segment
        final List<OrderedBlobEntry> segment = new ArrayList<>( jobEntries.values() );
        final S3ObjectsIoRequest objects =
                constructOrderedObjectsIoRequest( JobRequestType.VERIFY, segment );
        if ( performVerify( segment, objects ) )
        {
            m_verifiedBlobs.addAll( jobEntries.keySet() );
            doNotTreatReadyReturnValueAsFailure();
        }

        return BlobStoreTaskState.READY;
    }
    
    
    private BlobStoreTaskState runPartialVerify( final int percentOfEndOfTapeToVerify )
    {
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final WhereClause blobFilter = Require.exists( 
                BlobTape.class,
                BlobObservable.BLOB_ID, 
                Require.beanPropertyEquals( BlobTape.TAPE_ID, getTapeId() ) );
        final long totalBlobData = getServiceManager().getRetriever( Blob.class ).getSum( 
                Blob.LENGTH, 
                blobFilter );
        
        LOG.info( "Logical data on tape: " + bytesRenderer.render( totalBlobData ) );
        final long tapeRawCapacity = ( null == getTape().getTotalRawCapacity() ) ? 
                0 
                : getTape().getTotalRawCapacity();
        LOG.info( "Physical capacity of tape: " + bytesRenderer.render( tapeRawCapacity ) );
        
        final long lastBytesToVerify =
                Math.max( totalBlobData, tapeRawCapacity ) * percentOfEndOfTapeToVerify / 100;
        LOG.info( "Minimum logical data to verify at end of tape: "
                  + bytesRenderer.render( lastBytesToVerify ) );
        
        final Map< UUID, Long > blobSizes = new HashMap<>();
        try ( final EnhancedIterable< Blob > iterator = 
                getServiceManager().getRetriever( Blob.class ).retrieveAll( blobFilter ).toIterable() )
        {
            for ( final Blob blob : iterator )
            {
                blobSizes.put( blob.getId(), blob.getLength() );
            }
        }
        
        final List< UUID > sortedBlobIds = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( BlobTape.ORDER_INDEX, SortBy.Direction.ASCENDING );
        ordering.add( BlobTape.TAPE_ID, SortBy.Direction.ASCENDING );
        try ( final EnhancedIterable< BlobTape > iterator =
                getServiceManager().getRetriever( BlobTape.class ).retrieveAll( Query.where( 
                        Require.beanPropertyEquals( BlobTape.TAPE_ID, getTapeId() ) )
                        .orderBy( ordering ) ).toIterable() )
        {
            for ( final BlobTape bt : iterator )
            {
                sortedBlobIds.add( bt.getBlobId() );
            }
        }
        Collections.reverse( sortedBlobIds );
        
        final Set< UUID > blobsToVerify = new HashSet<>();
        long bytesToVerify = lastBytesToVerify;
        while ( 0 < bytesToVerify && !sortedBlobIds.isEmpty() )
        {
            final UUID blobId = sortedBlobIds.remove( 0 );
            blobsToVerify.add( blobId );
            bytesToVerify -= blobSizes.get( blobId );
        }
        
        final Map< UUID, OrderedBlobEntry> jobEntries = createJobEntries( new ArrayList<>( blobsToVerify ) );
        final boolean retval = performVerify( 
                new ArrayList<>( jobEntries.values() ),
                constructOrderedObjectsIoRequest( JobRequestType.VERIFY, new ArrayList<>( jobEntries.values() ) ) );
        if ( retval )
        {
            getServiceManager().getService( TapeService.class ).update(
                    getTape().setPartiallyVerifiedEndOfTape( new Date() ).setVerifyPending( null ),
                    Tape.PARTIALLY_VERIFIED_END_OF_TAPE, Tape.VERIFY_PENDING );
        }
        
        return ( retval ) ? BlobStoreTaskState.COMPLETED : BlobStoreTaskState.READY;
    }
    
    
    /**
     * @return Map< blob id, job entry >
     */
    private Map< UUID, OrderedBlobEntry> createJobEntries(final List< UUID > orderedBlobIds )
    {
        final Map< UUID, OrderedBlobEntry> retval = new HashMap<>();
        final Set< UUID > blobIds = new HashSet<>( orderedBlobIds );
        orderedBlobIds.clear();
    
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( BlobTape.ORDER_INDEX, SortBy.Direction.ASCENDING );
        ordering.add( BlobTape.TAPE_ID, SortBy.Direction.ASCENDING );
        final LimitableRetrievable query = Query
                .where( Require.all( 
                        Require.beanPropertyEquals( BlobTape.TAPE_ID, getTapeId() ),
                        Require.not( Require.exists( ObsoleteBlobTape.class, Identifiable.ID, Require.nothing() ) ),
                        ( !blobIds.isEmpty() ) ? 
                                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds )
                                : null ) )
                .orderBy( ordering );
        Validations.verifyNotNull( "Shut up CodePro", query );
        try ( final EnhancedIterable< BlobTape > btIterable =
                getServiceManager().getRetriever( BlobTape.class ).retrieveAll( query ).toIterable() )
        {
            for ( final BlobTape bt : btIterable )
            {
                if ( m_verifiedBlobs.contains( bt.getBlobId() ) )
                {
                    continue;
                }
                
                retval.put( 
                        bt.getBlobId(), 
                        (OrderedBlobEntry)BeanFactory.newBean( OrderedBlobEntry.class )
                        .setOrderIndex( bt.getOrderIndex() )
                        .setBlobId( bt.getBlobId() ) );
                orderedBlobIds.add( bt.getBlobId() );
                if ( m_maxSegmentCount == retval.size() )
                {
                    break;
                }
            }
        }
        
        return retval;
    }
    
    
    private boolean performVerify(final List<OrderedBlobEntry> jobEntries, final S3ObjectsIoRequest objects )
    {
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final long bytesToRead = getTotalWorkInBytes( objects );
        final Duration duration = new Duration();
        final String dataDescription = 
                jobEntries.size() + " blobs (" + bytesRenderer.render( bytesToRead ) + ")";
        LOG.info( "Will verify " + dataDescription + " [" +  m_verifiedBlobs.size() 
                  + " blobs verified so far]..." );
        
        final BlobIoFailures failures;
        try
        {
            failures = getDriveResource().verifyData(
                    TapeTaskUtils.buildVerifyObjectsPayload( objects ) ).get( Timeout.VERY_LONG );
            if ( 0 != failures.getFailures().length )
            {
                final Set< UUID > failedBlobIds = new HashSet<>();
                final Map< BlobIoFailureType, List< UUID > > failuresByCause = new HashMap<>();
                for ( final BlobIoFailure failure : failures.getFailures() )
                {
                    final UUID blobId = failure.getBlobId();
                    failedBlobIds.add( blobId );
                    
                    if ( !failuresByCause.containsKey( failure.getFailure() ) )
                    {
                        failuresByCause.put( failure.getFailure(), new ArrayList<>() );
                    }
                    failuresByCause.get( failure.getFailure() ).add( blobId );
                }

                final RuntimeException readFailuresEx = new RuntimeException(  
                        "Failed to verify " + failedBlobIds.size() + " blobs on tape due to " 
                        + failuresByCause.keySet() + ": " 
                        + LogUtil.getShortVersion( failuresByCause.toString() ) );
                LOG.warn( "Failed to verify " + failedBlobIds.size() + " blobs from tape.", readFailuresEx );
                m_tapeFailureManagement.registerFailure(
                        getTapeId(),
                        TapeFailureType.BLOB_READ_FAILED,
                        readFailuresEx );

                m_drivesFailedOn.add(getDriveId());
                if ( ++m_failureCount < MAX_FAILURES && m_drivesFailedOn.size() < MAX_DRIVES_TO_FAIL_ON )
                {
                    return false;
                }
                final BeansServiceManager transaction = getServiceManager().startTransaction();
                try
                {
                    transaction.getService( BlobTapeService.class ).blobsSuspect(
                            "failures verifying blobs",
                            getServiceManager().getRetriever( BlobTape.class ).retrieveAll( Require.all( 
                                    Require.beanPropertyEquals( BlobTape.TAPE_ID, getTapeId() ),
                                    Require.beanPropertyEqualsOneOf( 
                                            BlobObservable.BLOB_ID, failedBlobIds ) ) ).toSet() );
                    transaction.commitTransaction();
                    m_tapeFailureManagement.resetBlobReadFailuresWhenBlobMarkedSuspect( getTapeId() );
                    m_failureCount = 0;
                    m_drivesFailedOn.clear();
                }
                finally
                {
                    transaction.closeTransaction();
                }
            } else {
                m_tapeFailureManagement.resetFailures(
                        getTapeId(),
                        getDriveId(),
                        TapeFailureType.BLOB_READ_FAILED);
            }
            m_ltfsErrorFailureCount = 0;
            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.VERIFY_FAILED);
        }
        catch ( final RpcException ex )
        {
            if ( TapeResourceFailureCode.LTFS_ERROR.toString().equals( ex.getFailureType().getCode() )
                    && ++m_ltfsErrorFailureCount > MAX_FAILURES )
            {
                LOG.warn( dataDescription + " failed verify due to LTFS error max times on tape at "
                        + bytesRenderer.render( bytesToRead, duration) + "." );

                // mark blobs suspect
                final BeansServiceManager transaction = getServiceManager().startTransaction();
                try
                {
                    final List<UUID> suspectBlobIds = jobEntries.stream()
                            .map(BlobObservable::getBlobId)
                            .collect(Collectors.toList());

                    transaction.getService( BlobTapeService.class ).blobsSuspect(
                            "LTFS error when attempting to verify blobs",
                            getServiceManager().getRetriever( BlobTape.class ).retrieveAll( Require.all(
                                    Require.beanPropertyEquals( BlobTape.TAPE_ID, getTapeId() ),
                                    Require.beanPropertyEqualsOneOf(
                                            BlobObservable.BLOB_ID, suspectBlobIds ) ) ).toSet() );
                    transaction.commitTransaction();
                    m_tapeFailureManagement.resetVerifyFailuresWhenBlobMarkedSuspect(getTapeId());
                    m_ltfsErrorFailureCount = 0;
                }
                finally
                {
                    transaction.closeTransaction();
                }
            } else {
                m_tapeFailureManagement.registerFailure(
                        getTapeId(),
                        TapeFailureType.VERIFY_FAILED,
                        ex );
                throw ex;
            }
        }
        catch ( final RuntimeException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    TapeFailureType.VERIFY_FAILED,
                    ex );
            throw ex;
        }
        LOG.info( dataDescription + " verified on tape at " 
                  + bytesRenderer.render( bytesToRead, duration ) + "." );
        return true;
    }

    @Override
    public boolean allowMultiplePerTape() {
        return false;
    }


    @Override
    public String getDescription()
    {
        return "Verify Tape " + m_defaultTapeId;
    }

    
    private volatile DataPathBackend m_dataPathBackend;
    private final Set< UUID > m_verifiedBlobs = new HashSet<>();
    private final int m_maxSegmentCount;
    private int m_failureCount = 0;
    private int m_ltfsErrorFailureCount = 0;
    private Set<UUID> m_drivesFailedOn = new HashSet<>();
    private static int MAX_DRIVES_TO_FAIL_ON = 3;
    private static int MAX_FAILURES = 4;
}
