/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.azuretarget;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.target.AzureTargetService;
import com.spectralogic.s3.common.rpc.target.AzureConnection;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Validations;

public final class DefaultAzureConnectionFactory implements AzureConnectionFactory
{
    public DefaultAzureConnectionFactory( final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        m_servicesManager = serviceManager;
        m_service = serviceManager.getService( AzureTargetService.class );
    }
    
    
    public AzureConnection connect( final AzureTarget target )
    {
        Validations.verifyNotNull( "Target", target );
        
        AzureConnection retval = null;
        try
        {
            retval = new AzureConnectionImpl( target ,m_servicesManager);
            if ( TargetState.ONLINE != target.getState() )
            {
                m_service.update( target.setState( TargetState.ONLINE ), ReplicationTarget.STATE );
            }
        }
        catch ( final Exception ex )
        {
            if ( TargetState.OFFLINE != target.getState() )
            {
                m_service.update( target.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
            }
            throw ExceptionUtil.toRuntimeException( ex );
        }
        
        return retval;
    }
    
    
    private final AzureTargetService m_service;
    private final BeansServiceManager m_servicesManager;
}
