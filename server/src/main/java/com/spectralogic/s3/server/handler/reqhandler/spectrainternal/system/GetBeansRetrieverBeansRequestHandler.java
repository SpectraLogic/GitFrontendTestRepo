/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system;

import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.service.api.BeansRetriever;

public final class GetBeansRetrieverBeansRequestHandler extends BaseRequestHandler
{
    public GetBeansRetrieverBeansRequestHandler()
    {
        super( new InternalAccessOnlyAuthenticationStrategy(), 
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.SHOW, RestDomainType.BEANS_RETRIEVER ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final String typeName = request.getRestRequest().getIdAsString();
        try
        {
            return BeanServlet.serviceGet( params, BeanUtils.sort(
                    retrieverForTypeName( params, typeName ).retrieveAll().toSet() ) );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to retrieve beans for type " + typeName + ".", ex );
        }
    }
    
    
    @SuppressWarnings( "unchecked" )
    private < T extends SimpleBeanSafeToProxy & Identifiable > BeansRetriever< T > retrieverForTypeName(
            final CommandExecutionParams params,
            final String typeName )
    {
        final Class< T > type;
        try
        {
            type = (Class< T >)Class.forName( typeName );
        }
        catch ( final ClassNotFoundException ex )
        {
            throw new RuntimeException( ex );
        }
        return params.getServiceManager().getRetriever( type );
    }
}
