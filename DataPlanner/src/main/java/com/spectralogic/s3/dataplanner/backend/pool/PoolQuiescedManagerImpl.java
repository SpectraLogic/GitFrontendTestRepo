/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.service.ds3.DataPathBackendService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

final class PoolQuiescedManagerImpl implements PoolQuiescedManager
{
    PoolQuiescedManagerImpl( final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        Validations.verifyNotNull( "Service manager", m_serviceManager );
    }


    synchronized public void verifyNotQuiesced( final UUID poolId )
    {
        final String failure = getQuiescedCause( poolId );
        if ( null == failure )
        {
            return;
        }
        
        throw new IllegalStateException( failure );
    }


    public boolean isQuiesced( final UUID poolId )
    {
        return ( null != getQuiescedCause( poolId ) );
    }
    
    
    synchronized public String getQuiescedCause( final UUID poolId )
    {
        Validations.verifyNotNull( "Pool id", poolId );
        
        final Pool pool = m_serviceManager.getService( PoolService.class ).attain( poolId );
        if ( Quiesced.NO != pool.getQuiesced() )
        {
            return "Pool's quiesced state is " + pool.getQuiesced();
        }
        if ( !m_serviceManager.getService( DataPathBackendService.class ).isActivated() )
        {
            return "Backend has not been activated.";
        }
        return null;
    }
    
    
    private final BeansServiceManager m_serviceManager;
}
