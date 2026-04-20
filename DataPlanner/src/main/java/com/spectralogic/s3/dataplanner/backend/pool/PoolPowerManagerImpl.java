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

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockingException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

final class PoolPowerManagerImpl implements PoolPowerManager
{
    PoolPowerManagerImpl( 
            final PoolService poolService, 
            final PoolEnvironmentResource poolEnvironmentResource )
    {
        m_poolService = poolService;
        m_poolEnvironmentResource = poolEnvironmentResource;
        Validations.verifyNotNull( "Pool retriever", m_poolService );
        Validations.verifyNotNull( "Pool environment resource", m_poolEnvironmentResource );
    }
    
    
    synchronized public void powerOn( final UUID poolId )
    {
        Validations.verifyNotNull( "Pool", poolId );
        try
        {
            if ( m_poweredOnPools.contains( poolId ) )
            {
                return;
            }

            final Pool pool = m_poolService.attain( poolId );
            m_poolEnvironmentResource.powerOn( pool.getGuid() ).get( Timeout.DEFAULT );
            m_poolService.update( pool.setPoweredOn( true ), PoolObservable.POWERED_ON );
            m_poweredOnPools.add( poolId );
        }
        catch ( final RuntimeException ex )
        {
            throw new PoolLockingException(
                    "Failed to power on pool " + poolId + ".", ex );
        }
    }
    
    
    synchronized public void powerOff( final UUID poolId )
    {
        Validations.verifyNotNull( "Pool", poolId );
        if ( !m_poweredOnPools.contains( poolId ) )
        {
            return;
        }
        
        m_poolEnvironmentResource.powerOff( 
                m_poolService.attain( poolId ).getGuid() ).get( Timeout.DEFAULT );
        m_poweredOnPools.remove( poolId );
    }
    
    
    // Note: when we come up, we quiesce the tape environment resource which effectively powers off all pools
    private final Set< UUID > m_poweredOnPools = new HashSet<>();
    private final PoolService m_poolService;
    private final PoolEnvironmentResource m_poolEnvironmentResource;
}
