/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.DataOfflineablePublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseRegisterPublicCloudTargetRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class RegisterS3TargetRequestHandler
    extends BaseRegisterPublicCloudTargetRequestHandler< S3Target >
{
    public RegisterS3TargetRequestHandler()
    {
        super( S3Target.class,
               RestDomainType.S3_TARGET );

        registerBeanProperties( 
                S3Target.REGION,
                S3Target.ACCESS_KEY,
                S3Target.SECRET_KEY,
                S3Target.DATA_PATH_END_POINT,
                S3Target.PROXY_DOMAIN,
                S3Target.PROXY_HOST,
                S3Target.PROXY_PASSWORD,
                S3Target.PROXY_PORT,
                S3Target.PROXY_USERNAME,
                S3Target.RESTRICTED_ACCESS,
                DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB,
                DataOfflineablePublicCloudReplicationTarget.STAGED_DATA_EXPIRATION_IN_DAYS,
                PublicCloudReplicationTarget.NAMING_MODE );
    }


    @Override
    protected void validateBeanForCreation( final CommandExecutionParams params, final S3Target target )
    {
        super.validateBeanForCreation( params, target );
        //This check serves as a unique constraint on access key region pairs that only gets applied
        //when both targets are have no endpoint defined to override the region specification.
        if (null == target.getDataPathEndPoint() &&
                0 < params.getServiceManager().getRetriever( S3Target.class ).getCount(
                    Require.all(
                            Require.beanPropertyEquals(
                                    S3Target.DATA_PATH_END_POINT,
                                    null ),
                            Require.beanPropertyEquals(
                                    S3Target.REGION,
                                    target.getRegion() ),
                            Require.beanPropertyEquals(
                                    S3Target.ACCESS_KEY,
                                    target.getAccessKey() ) ) ) )
        {
            throw new S3RestException( 
                    GenericFailure.CONFLICT, "An S3 target with access key \"" + target.getAccessKey()
                    + "\" is already registered for region \"" + target.getRegion() + "\".");   
        }
        ModifyS3TargetRequestHandler.validateOfflineDataStagingWindow( target );
        ModifyS3TargetRequestHandler.validateStagedDataExpirationInDays( target );
    }


    @Override
    protected UUID createBean( final CommandExecutionParams params, final S3Target target )
    {
        target.setId( UUID.randomUUID() );
        return params.getTargetResource().registerS3Target( target ).get( Timeout.DEFAULT );
    }
}
