/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailuresInformation;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.object.PhysicalPlacementCalculator;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

abstract class BaseEjectStorageDomainRequestHandler extends BaseDaoTypedRequestHandler< Tape >
{
    protected BaseEjectStorageDomainRequestHandler( final boolean requireSuccessfulBlobParsing )
    {
        super( Tape.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_MODIFY,
                       RestOperationType.EJECT,
                       RestDomainType.TAPE ) );
        
        m_requireSuccessfulBlobParsing = requireSuccessfulBlobParsing;
        registerRequiredRequestParameters( RequestParameterType.STORAGE_DOMAIN );
        registerOptionalBeanProperties( 
                Tape.EJECT_LABEL, Tape.EJECT_LOCATION );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Tape queryParams = getBeanSpecifiedViaQueryParameters(
                params, AutoPopulatePropertiesWithDefaults.NO );
        final Set< UUID > blobIds = PhysicalPlacementCalculator.extractBlobsFromHttpRequest( 
                params, queryParams.getBucketId(), true, m_requireSuccessfulBlobParsing );
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
        final TapeFailuresInformation rpcResponse = 
                params.getTapeResource().ejectStorageDomain(
                        storageDomainId,
                        queryParams.getBucketId(),
                        queryParams.getEjectLabel(),
                        queryParams.getEjectLocation(),
                        ( null == blobIds ) ?
                                CollectionFactory.toArray( UUID.class, new HashSet< UUID >() ) 
                                : CollectionFactory.toArray( UUID.class, blobIds ) ).get( Timeout.LONG );
        if ( null == rpcResponse || 0 == rpcResponse.getFailures().length )
        {
            return BeanServlet.serviceModify( params, null );
        }
        return BeanServlet.serviceRequest( 
                params,
                207, // multiple statuses
                new TapeFailuresResponseBuilder( rpcResponse, params ).build() );
    }
    
    
    private final boolean m_requireSuccessfulBlobParsing;
}