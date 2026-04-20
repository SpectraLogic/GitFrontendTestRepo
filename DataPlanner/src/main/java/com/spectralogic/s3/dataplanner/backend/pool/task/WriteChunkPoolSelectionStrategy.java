/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.util.*;

import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.dataplanner.backend.frmwrk.WritesStalledSupport;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.WhereClause;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.orm.PoolRM;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.dataplanner.backend.frmwrk.CanAllocatePersistenceTargetSupport;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.render.BytesRenderer;

final class WriteChunkPoolSelectionStrategy
{

    WriteChunkPoolSelectionStrategy(final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        m_canAllocateSupport = new CanAllocatePersistenceTargetSupport( serviceManager );
        Validations.verifyNotNull( "Service manager", m_serviceManager );
    }

    
    UUID selectPool(
            final long fullSize,
            final UUID storageDomainId,
            final UUID bucketId,
            final Set< UUID > unavailablePoolIds )
    {
        final String chunkDescription = 
                new BytesRenderer().render(fullSize) + " (bucket " + bucketId + ")";

        UUID isolatedBucketId = null;
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

        final boolean allowedToAllocateNewPool = m_canAllocateSupport
                .allowedToTakeAnotherPersistenceTarget(fullSize, isolatedBucketId, storageDomainId);

        Pool candidate = null;
        if (allowedToAllocateNewPool) {
            candidate = selectPool(
                    fullSize, storageDomainId, isolatedBucketId, unavailablePoolIds, true );
            if (null != candidate) {
                final UUID sdmId = m_serviceManager.getService( StorageDomainService.class )
                        .selectAppropriateStorageDomainMember( candidate, storageDomainId );
                LOG.info("Can write " + chunkDescription + " to a new pool.");
                m_serviceManager.getService(PoolService.class).update(
                        candidate.setBucketId(isolatedBucketId)
                                .setStorageDomainMemberId(sdmId).setAssignedToStorageDomain(true),
                        PersistenceTarget.BUCKET_ID,
                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                        PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN);

                CanAllocatePersistenceTargetSupport.clearCachedBucketWriteOptimizationData();
            }
        }
        if (candidate == null) {
            candidate = selectPool(
                    fullSize, storageDomainId, isolatedBucketId, unavailablePoolIds, false);
        }
        if ( null != candidate )
        {
            WritesStalledSupport.INSTANCE.notStalled( null, storageDomainId, m_serviceManager );
            return candidate.getId();
        }

        if ( allowedToAllocateNewPool )
        {
            WritesStalledSupport.INSTANCE.stalled( null, storageDomainId, m_serviceManager );
        }

        LOG.info( "No pools available that will fit " + chunkDescription + ", which needs "
                + new BytesRenderer().render( fullSize ) + ".  Pools currently unavailable: "
                + LogUtil.getShortVersion( unavailablePoolIds, 5 ) );
        return null;
    }


    private Pool selectPool(
            final long bytesToWrite,
            final UUID storageDomainId,
            final UUID isolatedBucketId,
            final Set< UUID > unavailablePoolIds,
            final boolean allowDaoChanges)
    {
        Pool candidate = null;
        final List<WhereClause> filters = getPoolFiltersForStorageDomain(
                storageDomainId, isolatedBucketId, bytesToWrite, unavailablePoolIds, allowDaoChanges);
        for ( final WhereClause filter : filters )
        {
            final List< Pool > persistenceTargets =
                    m_serviceManager.getRetriever( Pool.class ).retrieveAll( filter ).toList();
            //NOTE: capacity info sorted by here will not include writes that are in progress. We track that info in
            //PoolLockSupportImpl and should ideally use that when sorting
            persistenceTargets.sort( new BeanComparator<>( Pool.class,
                    new BeanComparator.BeanPropertyComparisonSpecifiction( PoolObservable.AVAILABLE_CAPACITY, SortBy.Direction.DESCENDING,
                            null ) ) );
            if ( !persistenceTargets.isEmpty() )
            {
                if ( null == candidate)
                {
                    candidate = persistenceTargets.get( 0 );
                }
            }
        }
        return candidate;
    }


    private List< WhereClause > getPoolFiltersForStorageDomain(
            final UUID storageDomainId,
            final UUID isolatedBucketId,
            final long bytesToWrite,
            final Set< UUID > unavailablePoolIds,
            final boolean unallocated )
    {

        final List< WhereClause > retval = new ArrayList<>();
        retval.add( PersistenceTargetUtil.filterForWritablePools(
                isolatedBucketId,
                storageDomainId,
                bytesToWrite,
                unavailablePoolIds,
                unallocated ) );
        return retval;
    }
    
    
    private final BeansServiceManager m_serviceManager;
    private final CanAllocatePersistenceTargetSupport m_canAllocateSupport;
    private final static Logger LOG = Logger.getLogger( WriteChunkPoolSelectionStrategy.class );

}
