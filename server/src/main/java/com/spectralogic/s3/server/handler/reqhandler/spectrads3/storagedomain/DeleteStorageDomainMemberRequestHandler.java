/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class DeleteStorageDomainMemberRequestHandler
    extends BaseDeleteBeanRequestHandler< StorageDomainMember >
{
    public DeleteStorageDomainMemberRequestHandler()
    {
        super( StorageDomainMember.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.STORAGE_DOMAIN_MEMBER );
    }

    
    @Override
    protected void deleteBean( final CommandExecutionParams params, final StorageDomainMember bean )
    {
        params.getDataPolicyResource().deleteStorageDomainMember( bean.getId() ).get( Timeout.LONG );
    }
}
