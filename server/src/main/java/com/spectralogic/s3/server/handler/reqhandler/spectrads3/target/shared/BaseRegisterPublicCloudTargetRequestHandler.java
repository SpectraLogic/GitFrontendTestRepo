/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.lang.DatabasePersistable;

abstract public class BaseRegisterPublicCloudTargetRequestHandler
    < T extends PublicCloudReplicationTarget< T > & DatabasePersistable >
    extends BaseCreateBeanRequestHandler< T >
{
    protected BaseRegisterPublicCloudTargetRequestHandler(
            final Class< T > targetType,
            final RestDomainType restDomainType )
    {
        super( targetType,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               restDomainType );
    
        registerBeanProperties( 
                NameObservable.NAME,
                PublicCloudReplicationTarget.HTTPS,
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX,
                PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX,
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
    }
    
    
    @Override
    protected void validateBeanForCreation( final CommandExecutionParams params, final T target )
    {
        BaseModifyPublicCloudTargetRequestHandler.validateAutoVerificationFrequency( target );
    }
}
