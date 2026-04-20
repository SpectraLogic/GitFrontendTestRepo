/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.http.RequestType;

public final class HeadBucketRequestHandler extends BaseRequestHandler
{
    public HeadBucketRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                SystemBucketAccess.STANDARD,
                BucketAclPermission.LIST,
                AdministratorOverride.YES ), 
               new NonRestfulCanHandleRequestDeterminer( 
                RequestType.HEAD,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansServiceManager serviceManager = params.getServiceManager();
        
        serviceManager.getRetriever( Bucket.class )
                .attain( Bucket.NAME, request.getBucketName() );
        return BeanServlet.serviceGet( params, null );
    }
}
