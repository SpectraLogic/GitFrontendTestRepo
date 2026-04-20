/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.orm.PoolRM;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.dataplanner.backend.frmwrk.CanAllocatePersistenceTargetSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockingException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.mock.InterfaceProxyFactory;

/**
 * Pools are automatically compacted as necessary, which means that we don't need to actually format or
 * reclaim any space on pools we mark as assignable.  We just need to mark these pools as being 
 * assignable to another storage domain.
 */
final class ReclaimPoolProcessor implements Runnable
{
    ReclaimPoolProcessor(
            final BeansServiceManager serviceManager, 
            final PoolLockSupport< PoolTask > lockSupport )
    {
        m_serviceManager = serviceManager;
        m_lockSupport = lockSupport;
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        Validations.verifyNotNull( "Lock support", m_lockSupport );
    }
    
    
    @Override
    synchronized public void run()
    {
        for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll( Require.all( 
                Require.beanPropertyEquals( Pool.STATE, PoolState.NORMAL ),
                Require.beanPropertyEquals( PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, Boolean.TRUE ),
                Require.not( Require.exists( 
                        BlobPool.class,
                        BlobPool.POOL_ID,
                        Require.nothing() ) ) ) ).toSet() )
        {
            reclaimPool( pool );
        }
    }
    
    
    private void reclaimPool( final Pool pool )
    {
        if ( null != pool.getStorageDomainMemberId() )
        {
            final StorageDomain storageDomain =
                    new PoolRM( pool, m_serviceManager ).getStorageDomainMember().getStorageDomain().unwrap();
            if ( storageDomain.isSecureMediaAllocation() )
            {
                if ( !m_unreclaimablePoolsLogged.contains( pool.getId() ) )
                {
                    m_unreclaimablePoolsLogged.add( pool.getId() );
                    LOG.info( "Pool " + pool.getId() 
                            + " would be reclaimed since there is no data remaining on it, " 
                            + "except that it has been allocated to storage domain " + storageDomain.getId() 
                            + ", which is configured for secure media allocation." );
                }
                return;
            }
        }
        
        final PoolTask lockHolder = InterfaceProxyFactory.getProxy( PoolTask.class, null );
        try
        {
            m_lockSupport.acquireExclusiveLock(
                    pool.getId(),
                    lockHolder );
            
            LOG.info( "Pool " + pool.getId() + " will be reclaimed since there is no data remaining on it." );
            m_unreclaimablePoolsLogged.remove( pool.getId() );
            m_serviceManager.getService( PoolService.class ).update( 
                    pool.setBucketId( null ).setStorageDomainMemberId( null ).setAssignedToStorageDomain( false ), 
                    PersistenceTarget.BUCKET_ID, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                    PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
            CanAllocatePersistenceTargetSupport.clearCachedBucketWriteOptimizationData();
        }
        catch ( final PoolLockingException ex )
        {
            LOG.info( "Pool " + pool.getId() + " would be reclaimed since there is no data remaining on it, "
                      + "except that it is locked at this time.  Will retry later.", ex );
            return;
        }
        finally
        {
            m_lockSupport.releaseLock( lockHolder );
        }
    }
    
    
    private final Set< UUID > m_unreclaimablePoolsLogged = new HashSet<>();
    private final BeansServiceManager m_serviceManager;
    private final PoolLockSupport< PoolTask > m_lockSupport;
    
    private final static Logger LOG = Logger.getLogger( ReclaimPoolProcessor.class );
}
