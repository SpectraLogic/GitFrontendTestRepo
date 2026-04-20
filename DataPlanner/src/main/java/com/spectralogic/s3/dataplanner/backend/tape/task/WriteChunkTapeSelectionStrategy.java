/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.*;

import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.dataplanner.backend.frmwrk.CanAllocatePersistenceTargetSupport;
import com.spectralogic.s3.dataplanner.backend.frmwrk.WritesStalledSupport;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.render.BytesRenderer;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

public final class WriteChunkTapeSelectionStrategy
{
    public WriteChunkTapeSelectionStrategy( final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        m_canAllocateSupport = new CanAllocatePersistenceTargetSupport( m_serviceManager );
    }

    public UUID selectTape(
            final long fullSize,
            final UUID storageDomainId,
            final UUID bucketId,
            final TapeAvailability tapeAvailability,
            final boolean allocateSelection)
    {
        final Set< UUID > unavailableTapeIds = tapeAvailability.getAllUnavailableTapes();
        Tape candidate;
        final UUID tapePartitionId = tapeAvailability.getTapePartitionId();

        final String chunksDescription = new BytesRenderer().render(fullSize) + " (bucket " + bucketId + ")";

        UUID isolatedBucketId;
        try
        {
            isolatedBucketId = PersistenceTargetUtil.getIsolatedBucketId(
                    bucketId, storageDomainId, m_serviceManager );
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "A persistence rule was deleted so that bucket "
                    + bucketId + " no longer targets storage domain " + storageDomainId + "." );
            return null;
        }

        candidate = selectTape(
                storageDomainId,
                isolatedBucketId,
                tapePartitionId,
                tapeAvailability.getTapeInDrive(),
                fullSize,
                unavailableTapeIds,
                false
        );

        if (candidate == null) {
            final boolean allowedToAllocateNewTape = m_canAllocateSupport
                    .allowedToTakeAnotherPersistenceTarget(fullSize, isolatedBucketId, storageDomainId);

            if (allowedToAllocateNewTape) {
                candidate = selectTape(
                        storageDomainId,
                        isolatedBucketId,
                        tapePartitionId,
                        tapeAvailability.getTapeInDrive(),
                        fullSize,
                        unavailableTapeIds,
                        true
                );
                if (candidate != null && allocateSelection) {
                    final UUID sdmId = m_serviceManager.getService(StorageDomainService.class)
                            .selectAppropriateStorageDomainMember(candidate, storageDomainId);
                    LOG.info("Can write " + chunksDescription + " to a new tape.");
                    m_serviceManager.getService(TapeService.class).update(
                            candidate.setBucketId(isolatedBucketId)
                                    .setStorageDomainMemberId(sdmId).setAssignedToStorageDomain(true),
                            PersistenceTarget.BUCKET_ID,
                            PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                            PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN);
                    CanAllocatePersistenceTargetSupport.clearCachedBucketWriteOptimizationData();
                }
            }


            if ( allowedToAllocateNewTape && null == candidate )
            {
                WritesStalledSupport.INSTANCE.stalled( tapePartitionId, storageDomainId, m_serviceManager );
            }
        }

        if ( null != candidate )
        {
            WritesStalledSupport.INSTANCE.notStalled( tapePartitionId, storageDomainId, m_serviceManager );
            return candidate.getId();
        }


        return null;
    }


    synchronized public Tape selectTape(
            final UUID storageDomainId,
            final UUID isolatedBucketId,
            final UUID tapePartitionId,
            final UUID preferredTapeId,
            final long bytesToWrite,
            final Set<UUID> unavailableTapeIds,
            final boolean unallocated)
    {
        Tape retval = null;
        final List<WhereClause> filters = getTapeFiltersForStorageDomain(
                tapePartitionId, storageDomainId, isolatedBucketId, bytesToWrite, unavailableTapeIds, unallocated);
        for ( final WhereClause filter : filters )
        {
            final List< Tape > persistenceTargets =
                    m_serviceManager.getRetriever( Tape.class ).retrieveAll( filter ).toList();
            persistenceTargets.sort( new BeanComparator<>( Tape.class,
                    new BeanComparator.BeanPropertyComparisonSpecifiction( Tape.AVAILABLE_RAW_CAPACITY, SortBy.Direction.ASCENDING,
                            null ) ) );
            if ( !persistenceTargets.isEmpty() )
            {
                if ( null == retval )
                {
                    retval = persistenceTargets.get( 0 );
                }
            }
            if ( null != preferredTapeId)
            {
                for ( final Tape tape : persistenceTargets )
                {
                    if ( tape.getId().equals(preferredTapeId) )
                    {
                        retval = tape;
                    }
                }
            }
        }

        final Tape candidate = retval;
        if ( null == candidate )
        {
            return null;
        }
        return candidate;
    }


    private List< WhereClause > getTapeFiltersForStorageDomain(
            final UUID tapePartitionId,
            final UUID storageDomainId,
            final UUID isolatedBucketId,
            final long bytesToWrite,
            final Set< UUID > unavailableTapeIds,
            final boolean unallocated )
    {
        final List< WhereClause > retval = new ArrayList<>();
        retval.add( PersistenceTargetUtil.filterForWritableTapes(
                isolatedBucketId,
                storageDomainId,
                bytesToWrite,
                unavailableTapeIds,
                tapePartitionId,
                unallocated ) );
        return retval;
    }
    
    
    private final BeansServiceManager m_serviceManager;
    private final CanAllocatePersistenceTargetSupport m_canAllocateSupport;
    private final static Logger LOG = Logger.getLogger( WriteChunkTapeSelectionStrategy.class );
}
