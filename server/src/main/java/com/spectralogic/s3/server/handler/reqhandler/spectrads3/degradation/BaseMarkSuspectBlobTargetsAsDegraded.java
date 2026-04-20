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

import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.service.target.BlobTargetService;
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
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

abstract class BaseMarkSuspectBlobTargetsAsDegraded
    < S extends Identifiable & BlobTarget< ? >, BS extends BlobTargetService< ?, ? > > 
    extends BaseRequestHandler
{
    protected BaseMarkSuspectBlobTargetsAsDegraded(
            final Class< S > suspectBlobTargetType,
            final Class< BS > blobTargetServiceType,
            final RestDomainType restDomainType )
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_MODIFY, 
                       restDomainType ) );
        registerOptionalRequestParameters( RequestParameterType.FORCE );
        
        m_suspectBlobTargetType = suspectBlobTargetType;
        m_blobTargetServiceType = blobTargetServiceType;
        Validations.verifyNotNull( "Suspect blob target type", m_suspectBlobTargetType );
        Validations.verifyNotNull( "Blob target service type", m_blobTargetServiceType );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Set< UUID > ids = SuspectBlobUtil.extractIds( request );
        final Map< UUID, Set< UUID > > degradedBlobs = new HashMap<>();
        final BeansRetriever< S > retriever = 
                params.getServiceManager().getRetriever( m_suspectBlobTargetType );
        for ( final S degradedBlob : ( null == ids ) ?
                retriever.retrieveAll().toSet()
                : retriever.retrieveAll( ids ).toSet() )
        {
            if ( !degradedBlobs.containsKey( degradedBlob.getTargetId() ) )
            {
                degradedBlobs.put( degradedBlob.getTargetId(), new HashSet< UUID >() );
            }
            degradedBlobs.get( degradedBlob.getTargetId() ).add( degradedBlob.getBlobId() );
        }
        
        final BeansServiceManager transaction = params.getServiceManager().startTransaction();
        try
        {
            for ( final Map.Entry< UUID, Set< UUID > > e : degradedBlobs.entrySet() )
            {
                transaction.getService( m_blobTargetServiceType ).blobsLost(
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
    
    
    private final Class< S > m_suspectBlobTargetType;
    private final Class< BS > m_blobTargetServiceType;
}
