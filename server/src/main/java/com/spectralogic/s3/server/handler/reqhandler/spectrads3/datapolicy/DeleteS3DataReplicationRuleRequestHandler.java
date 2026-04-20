/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public class DeleteS3DataReplicationRuleRequestHandler
    extends BaseDeleteBeanRequestHandler< S3DataReplicationRule >
{
    public DeleteS3DataReplicationRuleRequestHandler()
    {
        super( S3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.S3_DATA_REPLICATION_RULE );
    }


    @Override
    protected void deleteBean( final CommandExecutionParams params, final S3DataReplicationRule bean )
    {
        params.getDataPolicyResource().deleteS3DataReplicationRule( bean.getId() ).get( Timeout.LONG );
    }
}
