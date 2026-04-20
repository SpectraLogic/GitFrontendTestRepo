/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.CloudNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.domain.target.DataOfflineablePublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.S3TargetBucketName;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseModifyPublicCloudTargetRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyS3TargetRequestHandler
    extends BaseModifyPublicCloudTargetRequestHandler< S3Target, S3DataReplicationRule, S3TargetBucketName >
{
    public ModifyS3TargetRequestHandler()
    {
        super( S3Target.class,
               S3DataReplicationRule.class,
               S3TargetBucketName.class,
               RestDomainType.S3_TARGET );

        registerOptionalBeanProperties(
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
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final S3Target target,
            final Set< String > modifiedProperties )
    {
        super.validateBeanToCommit( params, target, modifiedProperties );
        
        if ( modifiedProperties.contains(
                DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB ) )
        {
            validateOfflineDataStagingWindow( target );
        }
        if ( modifiedProperties.contains(
                DataOfflineablePublicCloudReplicationTarget.STAGED_DATA_EXPIRATION_IN_DAYS ) )
        {
            validateStagedDataExpirationInDays( target );
        }
    }
    
    
    static void validateOfflineDataStagingWindow( final S3Target target )
    {
        if ( 1 > target.getOfflineDataStagingWindowInTb() )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST, "Staging window must be at least 1TB." );
        }
        if ( 64 < target.getOfflineDataStagingWindowInTb() )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST, "Staging window cannot exceed 64TB." );
        }
    }
    
    
    static void validateStagedDataExpirationInDays( final S3Target target )
    {
        if ( 1 > target.getStagedDataExpirationInDays() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST, 
                    "Staged data expiration must be at least 1 day." );
        }
        if ( 365 < target.getStagedDataExpirationInDays() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST, 
                    "Staged data expiration cannot exceed 365 days." );
        }
    }
    

    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final S3Target target,
            final Set< String > modifiedProperties )
    {
    	if ( modifiedProperties.contains( PublicCloudReplicationTarget.NAMING_MODE ) &&
    			0 < params.getServiceManager().getService( BucketService.class).getCount( Require.exists(
		                Bucket.DATA_POLICY_ID, 
		                Require.exists( 
		                        S3DataReplicationRule.class, 
		                        DataPlacement.DATA_POLICY_ID,
		                        Require.beanPropertyEquals( 
		                        		DataReplicationRule.TARGET_ID, 
		                                target.getId() ) ) ) ) )
        {
            throw new S3RestException(
                    GenericFailure.CONFLICT, 
                    "Cannot modify the " + PublicCloudReplicationTarget.NAMING_MODE 
                    + " since buckets are using the this target." );
        }
    
    	if ( 0 < params.getServiceManager().getRetriever( DataPolicy.class ).getCount(
    			Require.all(
	        			Require.exists(
	        					S3DataReplicationRule.class,
	        					DataPlacement.DATA_POLICY_ID,
	        					Require.exists(
	        							DataReplicationRule.TARGET_ID,
	        							Require.beanPropertyEquals( Identifiable.ID, target.getId() ) ) ),
						Require.not(
								Require.beanPropertyEquals(
										DataPolicy.VERSIONING,
										VersioningLevel.NONE ) ) ) )
                && target.getNamingMode() == CloudNamingMode.AWS_S3 )
    	{
    		throw new S3RestException( 
                    GenericFailure.CONFLICT, 
                    CloudNamingMode.AWS_S3 + " naming mode is only compatible with versioning level "
                    		+ VersioningLevel.NONE + "." );
    	}
    
        params.getTargetResource().modifyS3Target( 
                target, 
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.DEFAULT );
    }
}
