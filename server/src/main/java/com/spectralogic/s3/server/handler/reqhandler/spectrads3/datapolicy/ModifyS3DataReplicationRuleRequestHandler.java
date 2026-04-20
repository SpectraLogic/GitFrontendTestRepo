/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.PublicCloudDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyS3DataReplicationRuleRequestHandler
    extends BaseModifyBeanRequestHandler< S3DataReplicationRule >
{
    public ModifyS3DataReplicationRuleRequestHandler()
    {
        super( S3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.S3_DATA_REPLICATION_RULE );

        registerOptionalBeanProperties(
                DataPersistenceRule.TYPE,
                DataReplicationRule.REPLICATE_DELETES,
                PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES,
                S3DataReplicationRule.INITIAL_DATA_PLACEMENT );
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final S3DataReplicationRule rule,
            final Set< String > modifiedProperties )
    {
    	if ( modifiedProperties.contains( PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES )
    			&& rule.getMaxBlobPartSizeInBytes() >  MAX_S3_UPLOAD_SIZE )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST,
                    "Max blob part size for an S3 target must not exceed " + MAX_S3_UPLOAD_SIZE + " bytes." );
        }
        params.getDataPolicyResource().modifyS3DataReplicationRule(
                rule,
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.LONG );
    }
    
    private final static long MAX_S3_UPLOAD_SIZE = 5L * 1024 * 1024 * 1024;  
}
