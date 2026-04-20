/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetBucketName;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseModifyPublicCloudTargetRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyAzureTargetRequestHandler
    extends BaseModifyPublicCloudTargetRequestHandler
            < AzureTarget, AzureDataReplicationRule, AzureTargetBucketName >
{
    public ModifyAzureTargetRequestHandler()
    {
        super( AzureTarget.class,
               AzureDataReplicationRule.class,
               AzureTargetBucketName.class,
               RestDomainType.AZURE_TARGET );

        registerOptionalBeanProperties(
                AzureTarget.ACCOUNT_NAME,
                AzureTarget.ACCOUNT_KEY );
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final AzureTarget target,
            final Set< String > modifiedProperties )
    {
        params.getTargetResource().modifyAzureTarget( 
                target, 
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.DEFAULT );
    }
}
