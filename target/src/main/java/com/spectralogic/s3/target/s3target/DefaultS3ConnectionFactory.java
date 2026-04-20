/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.s3target;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.CloudNamingMode;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.target.S3MultipartUploadService;
import com.spectralogic.s3.common.dao.service.target.S3TargetService;
import com.spectralogic.s3.common.rpc.target.S3Connection;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Validations;

public final class DefaultS3ConnectionFactory implements S3ConnectionFactory
{
    public DefaultS3ConnectionFactory( final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        m_serviceManager = serviceManager;
        m_service = serviceManager.getService( S3TargetService.class );
        m_uploadTracker = new UploadTracker(
        		serviceManager.getService( S3MultipartUploadService.class ),
        		serviceManager.getRetriever( Blob.class ) );
    }
    
    
    public S3Connection connect( final S3Target target )
    {
        Validations.verifyNotNull( "Target", target );
        
        S3Connection retval = null;
        try
        {
        	if ( CloudNamingMode.AWS_S3 == target.getNamingMode() )
        	{
        		retval = new S3NativeConnectionImpl( target, m_uploadTracker, m_serviceManager );
        	}
        	else
        	{
        		retval = new S3ConnectionImpl( target, m_serviceManager );
        	}
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
    
    
    private final S3TargetService m_service;
    private final UploadTracker m_uploadTracker;
    private BeansServiceManager m_serviceManager;
}
