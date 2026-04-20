/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import java.io.InputStream;
import java.util.Properties;

import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.ExcludeRequestHandlerResponseDocumentation;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

@ExcludeRequestHandlerResponseDocumentation
public final class GetRequestHandlersFormalApiContractRequestHandler extends BaseRequestHandler
{
    public GetRequestHandlersFormalApiContractRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.NONE ), 
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.LIST,
                       RestDomainType.REQUEST_HANDLER_CONTRACT ) );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final InputStream propertiesIs = GetRequestHandlersRequestHandler.class.getResourceAsStream(
                "/requesthandlerresponses.props" );
        final Properties properties = new Properties();
        try
        {
            properties.load( propertiesIs );
            propertiesIs.close();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to load request handler documentation.", ex );
        }
        
        return BeanServlet.serviceGet( params, new ApiContractGenerator( properties ).getContract() );
    }
}
