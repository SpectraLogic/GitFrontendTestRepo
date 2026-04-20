/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class MarkSuspectBlobPoolsAsDegradedRequestHandler extends BaseRequestHandler
{
    public MarkSuspectBlobPoolsAsDegradedRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_MODIFY, 
                       RestDomainType.SUSPECT_BLOB_POOL ) );
        registerOptionalRequestParameters( RequestParameterType.FORCE );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Set< UUID > ids = SuspectBlobUtil.extractIds( request );
        final Map< UUID, Set< UUID > > degradedBlobs = new HashMap<>();
        final BeansRetriever< SuspectBlobPool > retriever = 
                params.getServiceManager().getRetriever( SuspectBlobPool.class );
        for ( final SuspectBlobPool degradedBlob : ( null == ids ) ?
                retriever.retrieveAll().toSet()
                : retriever.retrieveAll( ids ).toSet() )
        {
            if ( !degradedBlobs.containsKey( degradedBlob.getPoolId() ) )
            {
                degradedBlobs.put( degradedBlob.getPoolId(), new HashSet< UUID >() );
            }
            degradedBlobs.get( degradedBlob.getPoolId() ).add( degradedBlob.getBlobId() );
        }
        
        final BeansServiceManager transaction = params.getServiceManager().startTransaction();
        try
        {
            for ( final Map.Entry< UUID, Set< UUID > > e : degradedBlobs.entrySet() )
            {
                transaction.getService( BlobPoolService.class ).blobsLost(
                        "user confirmed suspected degradation",
                        e.getKey(),
                        e.getValue() );
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        return BeanServlet.serviceDelete( params, null );
    }
}
