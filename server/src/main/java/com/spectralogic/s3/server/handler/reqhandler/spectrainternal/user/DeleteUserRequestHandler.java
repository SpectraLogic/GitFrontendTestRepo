/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.user;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class DeleteUserRequestHandler extends BaseDeleteBeanRequestHandler< User >
{
    public DeleteUserRequestHandler()
    {
        // users are managed by the management path, so only allow internal requests to delete them
        super( User.class,
               new InternalAccessOnlyAuthenticationStrategy(),
               RestDomainType.USER_INTERNAL );
    }

    
    @Override
    protected void deleteBean( final CommandExecutionParams params, final User user )
    {
        params.getTargetResource().deleteUser( 
                null != params.getRequest().getHttpRequest().getHeader( 
                        S3HeaderType.REPLICATION_SOURCE_IDENTIFIER ),
                user.getId() ).get( Timeout.DEFAULT );
    }
}
