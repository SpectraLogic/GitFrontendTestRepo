/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;


import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

public final class GetPhysicalPlacementForObjectsRequestHandler extends BaseDaoTypedRequestHandler< Tape >
{
    public GetPhysicalPlacementForObjectsRequestHandler()
    {
        super( Tape.class,
               new BucketAuthorizationStrategy(
                SystemBucketAccess.STANDARD,
                BucketAclPermission.LIST, 
                AdministratorOverride.YES ), 
               new RestfulCanHandleRequestDeterminer( 
                RestOperationType.GET_PHYSICAL_PLACEMENT, 
                RestDomainType.BUCKET ) );
        
        registerOptionalRequestParameters( RequestParameterType.STORAGE_DOMAIN );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {

        final UUID storageDomainId;
        if (request.hasRequestParameter( RequestParameterType.STORAGE_DOMAIN ) )
        {
            storageDomainId = params.getServiceManager().getService( StorageDomainService.class ).attain( 
                    request.getRequestParameter( RequestParameterType.STORAGE_DOMAIN ).getString() ).getId();
        }
        else
        {
            storageDomainId = null;
        }
        return BeanServlet.serviceGet( 
                params, 
                new PhysicalPlacementCalculator( 
                        storageDomainId,
                        params,
                        false,
                        false ).getResult() );
    }
}
