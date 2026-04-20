/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.CloudNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacementRuleState;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.PublicCloudDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreateS3DataReplicationRuleRequestHandler
    extends BaseCreateBeanRequestHandler< S3DataReplicationRule >
{
    public CreateS3DataReplicationRuleRequestHandler()
    {
        super( S3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.S3_DATA_REPLICATION_RULE );

        registerBeanProperties(
                DataPlacement.DATA_POLICY_ID, 
                DataReplicationRule.TARGET_ID,
                DataReplicationRule.TYPE,
                DataReplicationRule.REPLICATE_DELETES,
                PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES,
                S3DataReplicationRule.INITIAL_DATA_PLACEMENT );
    }


    @Override
    protected void prepareBeanForCreation( final S3DataReplicationRule rule )
    {
        rule.setId( UUID.randomUUID() );
        rule.setState( DataPlacementRuleState.NORMAL );
    }


    @Override
    protected UUID createBean( final CommandExecutionParams params, final S3DataReplicationRule bean )
    {
    	final S3Target target =
    			params.getServiceManager().getRetriever( S3Target.class ).attain( bean.getTargetId() );
    	final DataPolicy dataPolicy =
    			params.getServiceManager().getRetriever( DataPolicy.class ).attain( bean.getDataPolicyId() );
		if ( CloudNamingMode.AWS_S3 == target.getNamingMode() && VersioningLevel.NONE != dataPolicy.getVersioning() )
		{
			throw new S3RestException( 
                    GenericFailure.CONFLICT, 
                    CloudNamingMode.AWS_S3 + " naming mode cannot be used with versioning level "
                    		+ dataPolicy.getVersioning() + "." );
		}
        return params.getTargetResource().createS3DataReplicationRule( bean ).get( Timeout.LONG );
    }
}
