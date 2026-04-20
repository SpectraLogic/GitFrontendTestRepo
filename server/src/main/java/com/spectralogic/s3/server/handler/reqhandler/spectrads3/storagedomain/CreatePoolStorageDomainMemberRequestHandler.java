/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreatePoolStorageDomainMemberRequestHandler 
    extends BaseCreateBeanRequestHandler< StorageDomainMember >
{
    public CreatePoolStorageDomainMemberRequestHandler()
    {
        super( StorageDomainMember.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.STORAGE_DOMAIN_MEMBER );
        
        registerRequiredBeanProperties(
                StorageDomainMember.STORAGE_DOMAIN_ID,
                StorageDomainMember.POOL_PARTITION_ID );
        registerOptionalBeanProperties( 
                StorageDomainMember.WRITE_PREFERENCE );
    }


    @Override
    protected UUID createBean( final CommandExecutionParams params, final StorageDomainMember bean )
    {
        bean.setId( UUID.randomUUID() );
        return params.getDataPolicyResource().createStorageDomainMember( bean ).get( Timeout.LONG );
    }
}
