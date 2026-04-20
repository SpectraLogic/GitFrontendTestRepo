/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.auth;

import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.find.FlagDetector;
import com.spectralogic.util.http.HttpRequest;

/**
 * Authentication strategy for requests that are only allowed to be executed as internal requests.
 */
public class InternalAccessOnlyAuthenticationStrategy implements AuthenticationStrategy
{
    final protected boolean isRequestInternal( final HttpRequest httpRequest )
    {
        return isRequest(
                S3HeaderType.INTERNAL_REQUEST_REQUIRING_AUTH_BYPASS,
                httpRequest );
    }
    
    
    final protected boolean isRequestImpersonatingUser( final HttpRequest httpRequest )
    {
        return isRequest(
                S3HeaderType.IMPERSONATE_USER,
                httpRequest );
    }
    
    
    public static boolean isRequest( final S3HeaderType type, final HttpRequest httpRequest )
    {
        for ( final String header : httpRequest.getHeaders().keySet() )
        {
            if ( header.equalsIgnoreCase( type.getHttpHeaderName() ) )
            {
                final String remoteAddr = httpRequest.getRemoteAddr();
                if ( "127.0.0.1".equals( remoteAddr ) || "0:0:0:0:0:0:0:1".equals( remoteAddr ) || "::1".equals( remoteAddr )
                        || FlagDetector.isFlagSet( ENABLE_REMOTE_INTERNAL_REQUESTS_FLAG ) )
                {
                    return true;
                }
                throw new S3RestException( 
                        AWSFailure.ACCESS_DENIED,
                        "Non-local clients cannot request internal processing bypassing authentication." );
            }
        }
        return false;
    }
    

    @Override
    public void authenticate( final CommandExecutionParams commandExecutionParams )
    {
        authenticate( commandExecutionParams.getRequest().getHttpRequest() );
    }
    
    
    public void authenticate( final HttpRequest request )
    {
        if ( isRequestInternal( request ) )
        {
            return;
        }

        throw new S3RestException(
                AWSFailure.ACCESS_DENIED,
                "Request cannot be called from non-Spectra-internal clients.  Use the '" 
                + S3HeaderType.INTERNAL_REQUEST_REQUIRING_AUTH_BYPASS 
                + "' HTTP header for internal processing." );
    }
    
    
    private final static String ENABLE_REMOTE_INTERNAL_REQUESTS_FLAG = "enableremoteinternalrequests";
}
