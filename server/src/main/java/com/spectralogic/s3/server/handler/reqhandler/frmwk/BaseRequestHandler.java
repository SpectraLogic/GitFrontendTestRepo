/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.handler.auth.AuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.CanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.RequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.find.FlagDetector;
import com.spectralogic.util.lang.Validations;

public abstract class BaseRequestHandler implements RequestHandler
{
    protected BaseRequestHandler(
            final AuthenticationStrategy authenticationStrategy,
            final CanHandleRequestDeterminer canHandleRequestDeterminer )
    {
        m_authenticationStrategy = authenticationStrategy;
        m_canHandleRequestDeterminer = canHandleRequestDeterminer;
        Validations.verifyNotNull( "Authentication strategy", m_authenticationStrategy );
        Validations.verifyNotNull( "Can handle request determiner.", m_canHandleRequestDeterminer );
    }
    
    
    final protected void requiresContentLengthHttpHeader()
    {
        m_requiresContentLengthHttpHeader = true;
    }
    
    
    final public CanHandleRequestDeterminer getCanHandleRequestDeterminer()
    {
        return m_canHandleRequestDeterminer;
    }
    
    
    final public ServletResponseStrategy handleRequest( final CommandExecutionParams commandExecutionParams )
    {
        Validations.verifyNotNull( "Command execution params", commandExecutionParams );
        synchronized ( this )
        {
            if ( null == m_serviceManager )
            {
                m_serviceManager = commandExecutionParams.getServiceManager();
            }
        }
        if ( !FlagDetector.isFlagSet( DISABLE_AUTHENTICATION_FLAG ) )
        {  
            m_authenticationStrategy.authenticate( commandExecutionParams );
        }
        
        if ( m_requiresContentLengthHttpHeader 
                && null == commandExecutionParams.getRequest().getHttpRequest().getHeader(
                        S3HeaderType.CONTENT_LENGTH ) )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.CONTENT_LENGTH_REQUIRED,
                    S3HeaderType.CONTENT_LENGTH + " HTTP header is required." );
        }
        
        return handleRequestInternal( commandExecutionParams.getRequest(), commandExecutionParams );
    }
    

    protected abstract ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, final CommandExecutionParams params );
    
    

    final protected void registerOptionalRequestParameters( final RequestParameterType ... requestParameters )
    {
        m_canHandleRequestDeterminer.getQueryStringRequirement().registerOptionalRequestParameters(
                requestParameters );
    }
    
    
    final protected void registerRequiredRequestParameters( final RequestParameterType ... requestParameters )
    {
        m_canHandleRequestDeterminer.getQueryStringRequirement().registerRequiredRequestParameters(
                requestParameters );
    }
    
    
    private volatile boolean m_requiresContentLengthHttpHeader;
    protected CommandExecutionParams m_commandExecutionParams;
    private final CanHandleRequestDeterminer m_canHandleRequestDeterminer;
    private final AuthenticationStrategy m_authenticationStrategy;
    private BeansServiceManager m_serviceManager;
    
    protected final static Logger LOG = Logger.getLogger( BaseRequestHandler.class );
    private final static String DISABLE_AUTHENTICATION_FLAG = "disablerequesthandlerauthentication";
}
