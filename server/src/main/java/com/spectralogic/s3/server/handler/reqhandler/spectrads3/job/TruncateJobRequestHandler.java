/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

public class TruncateJobRequestHandler extends BaseRequestHandler
{
    public TruncateJobRequestHandler()
    {
        this( RestDomainType.JOB );
    }
    
    
    protected TruncateJobRequestHandler( final RestDomainType domain )
    {
        super( new BucketAuthorizationStrategy( 
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.JOB, 
                AdministratorOverride.YES ),
               new RestfulCanHandleRequestDeterminer( 
                RestActionType.DELETE,
                domain ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        return new CancelJobRequestHandler().handleRequestInternal( request, params );
    }
}
