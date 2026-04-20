/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.server;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.spectralogic.util.exception.*;
import com.spectralogic.util.log.LoggingUncaughtExceptionHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.domain.Request;
import com.spectralogic.util.net.rpc.domain.RequestInvocationResult;
import com.spectralogic.util.net.rpc.domain.Response;
import com.spectralogic.util.net.rpc.domain.RpcFrameworkErrorCode;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcLogger;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceUtil;
import com.spectralogic.util.net.tcpip.TcpIpServer;
import com.spectralogic.util.net.tcpip.TcpIpServerImpl;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkConnectionClosedException;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageSender;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessage;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageEncoder;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class RpcServerImpl extends BaseShutdownable implements RpcServer
{
    public RpcServerImpl( final int port )
    {
        m_server = new TcpIpServerImpl<>(
                port, 
                new IncomingRequestDispatcher(),
                new JsonNetworkMessageDecoder(),
                RpcLogger.SERVER_LOG );
        m_server.run();
        addShutdownListener( m_server );
        addShutdownListener( new RpcCallHandlerThreadPoolShutDownListener() );
    }
    
    
    private final class RpcCallHandlerThreadPoolShutDownListener extends CriticalShutdownListener
    {
        @Override
        public void shutdownOccurred()
        {
            m_rpcCallHandlerThreadPoolExecutor.shutdownNow();
        }
    } // end inner class def
    
    
    synchronized public void register( String instanceName, final RpcResource rpcResource )
    {
        if ( null == instanceName )
        {
            instanceName = "";
        }
        Validations.verifyNotNull( "RPC resource", rpcResource );
        verifyNotShutdown();


        final String type = RpcResourceUtil.getResourceName( rpcResource.getClass() );
        if ( !m_incomingRequestHandlers.containsKey( type ) )
        {
            m_incomingRequestHandlers.put( type, new ConcurrentHashMap< String, RpcResource >() );
        }
        
        if ( null != m_incomingRequestHandlers.get( type ).get( instanceName ) )
        {
            unregister( instanceName, rpcResource.getClass() );
        }
        m_incomingRequestHandlers.get( type ).put( instanceName, rpcResource );

        RpcLogger.SERVER_LOG.info( LogUtil.getLogMessageImportantHeaderBlock(
                "Ready to service requests: " + rpcResource ) );
    }
    
    
    private final class IncomingRequestDispatcher implements NetworkMessageHandler< JsonNetworkMessage >
    {
        public void handle(
                final JsonNetworkMessage message,
                final NetworkMessageSender networkMessageSender )
        {
            Request r = null;
            try
            {
                final Duration durationToUnmarshal = new Duration();
                r = JsonMarshaler.unmarshal( Request.class, message.getJson() );
                if ( null == r.getInstance() )
                {
                    r.setInstance( "" );
                }
                
                r.setType( NamingConventionType.UNDERSCORED.convert( r.getType() ) );
                r.setMethod( NamingConventionType.UNDERSCORED.convert( r.getMethod() ) );
                r.setInstance( NamingConventionType.UNDERSCORED.convert( r.getInstance() ) );
                
                if ( 0 < durationToUnmarshal.getElapsedMillis() )
                {
                    final String requestDescription = RpcLogger.getDescriptionForRequest( 
                            r.getId(), r.getType(), r.getInstance(), r.getMethod() );
                    RpcLogger.SERVER_LOG.info(
                            "Unmarshaled " + requestDescription + " in " + durationToUnmarshal + "." );
                }
                
                final RpcResource resource = getResource( r );
                m_rpcCallHandlerThreadPoolExecutor.submit( 
                        new RequestExecutor( r, resource, networkMessageSender ) );
            }
            catch ( final RuntimeException ex )
            {
                if (ex instanceof FailureTypeObservableException
                        && ((FailureTypeObservableException) ex).getFailureType() == RpcFrameworkErrorCode.RESOURCE_TYPE_NOT_FOUND) {
                    RpcLogger.SERVER_LOG.warn( "Failed to handle request: " + ex.getMessage());
                } else {
                    RpcLogger.SERVER_LOG.error( "Failed to handle request.", ex );
                }
                if ( null != r )
                {
                    new RequestExecutor( r, null, networkMessageSender ).handleLocalMethodInvocationFailure(
                            false,
                            new ArrayList<>(),
                            ex );
                }
            }
        }
    } // end inner class def
    
    
    synchronized private RpcResource getResource( final Request r )
    {
        final Map< String, RpcResource > resources = m_incomingRequestHandlers.get( r.getType() );
        if ( null == resources )
        {
            throw new FailureTypeObservableException(
                    RpcFrameworkErrorCode.RESOURCE_TYPE_NOT_FOUND,
                    r.getType() + " does not exist.  These types do exist: " 
                    + m_incomingRequestHandlers.keySet() );
        }
        
        final RpcResource retval = resources.get( r.getInstance() );
        if ( null == retval )
        {
            throw new FailureTypeObservableException(
                    RpcFrameworkErrorCode.RESOURCE_INSTANCE_NOT_FOUND,
                    r.getType() + " '" + r.getInstance() 
                    + "' does not exist.  These instances do exist: " + resources.keySet() );
        }
        
        return retval;
    }
    
    
    private final class RequestExecutor implements Runnable
    {
        private RequestExecutor(
                final Request request, 
                final RpcResource rpcResource,
                final NetworkMessageSender msgSender )
        {
            m_request = request;
            m_rpcResource = rpcResource;
            m_msgSender = msgSender;
        }
        
        
        public void run()
        {
            try
            {
                runInternal();
            }
            catch ( final Exception ex )
            {
                LOG.error( "Internal error occurred while attempting to process " + m_request + ".", ex );
            }
        }
        
        
        private void runInternal()
        {
            final String methodName = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( 
                    m_request.getMethod() );
            m_rpcResourceMethod = ReflectUtil.getMethod(
                    RpcResourceUtil.getApi( m_rpcResource.getClass() ), methodName );
            m_requestDescription = RpcLogger.getDescriptionForRequest(
                    m_request.getId(), m_request.getType(), m_request.getInstance(), methodName );
            
            final StringBuilder logMessage = new StringBuilder();
            logMessage.append( LogUtil.getLogMessageHeaderBlock( 
                    "Service " + m_requestDescription ) );
            logMessage.append( Platform.NEWLINE ).append( 
                    "Local Servicing Resource: " + m_rpcResource );
            logMessage.append( Platform.NEWLINE ).append( 
                    "Local Servicing Method: " + m_rpcResourceMethod.getName() );
            logMessage.append( Platform.NEWLINE ).append( "Request Parameters: " )
                      .append( LogUtil.getShortVersion( LogUtil.hideSecretsInJson(
                              JsonMarshaler.formatPretty( m_request.getParams() ) ) ) );
            RpcLogger.SERVER_LOG.info( logMessage );
            
            final String originalThreadName = Thread.currentThread().getName();
            try
            {
                Thread.currentThread().setName(
                        "RPC<" + m_request.getId() + ">" );
                LOG.debug( m_requestDescription
                          + " received for servicing on thread " + originalThreadName + "." );
                invokeLocalMethod();
            }
            finally
            {
                Thread.currentThread().setName( originalThreadName );
            }
        }
        
        
        private List< Object > getMethodInvokeParams()
        {
            try
            {
                final JSONArray array = new JSONArray( m_request.getParams() );
                final List< String > args = new ArrayList<>();
                for ( int i = 0; i < array.length(); ++i )
                {
                    args.add( array.getString( i ) );
                }
                
                return RpcResourceUtil.getMethodInvokeParams( m_rpcResourceMethod, args );
            }
            catch ( final Exception ex )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST, 
                        "Failed to parse method params.", 
                        ex );
            }
            finally
            {
                // Once we parse the method invoke params, we can release the request params from RAM
                m_request.setParams( null );
            }
        }
        
        
        private void invokeLocalMethod()
        {
            final MonitoredWork work = new MonitoredWork( 
                    StackTraceLogging.SHORT, 
                    "Service " + m_requestDescription ).withCustomLogger( RpcLogger.SERVER_LOG );
            boolean success = false;
            List< Object > methodInvokeParams = new ArrayList<>();
            try
            {
                methodInvokeParams = getMethodInvokeParams();
                for ( final Object param : methodInvokeParams )
                {
                    RpcResourceUtil.validateResponse( param, GenericFailure.BAD_REQUEST );
                }
                
                final RpcFuture< ? > retval = (RpcFuture< ? >)m_rpcResourceMethod.invoke( 
                        m_rpcResource, 
                        CollectionFactory.toArray( Object.class, methodInvokeParams ) );
                final Object response = ( null == retval ) ? null : retval.getWithoutBlocking();
                final Class< ? > expectedReturnType = ReflectUtil.toNonPrimitiveType( 
                        m_rpcResourceMethod.getAnnotation( RpcMethodReturnType.class ).value() );
                if ( null == response )
                {
                    final NullAllowed nullAllowed = m_rpcResourceMethod.getAnnotation( NullAllowed.class );
                    if ( null == nullAllowed && void.class != expectedReturnType )
                    {
                        throw new RuntimeException( 
                                "Method was supposed to return a non-null response of type " 
                                + expectedReturnType.getName()
                                + ", but returned null." );
                    }
                }
                else
                {
                    final Class< ? > actualReturnType = ReflectUtil.toNonPrimitiveType( response.getClass() );
                    if ( !expectedReturnType.isAssignableFrom( actualReturnType ) )
                    {
                        throw new RuntimeException( 
                                "Method was supposed to return a response of type " 
                                + expectedReturnType.getName()
                                + ", but returned " + response + " of type " + actualReturnType.getName()
                                + "." );
                    }
                }
                final String stringReturnValue = MarshalUtil.getStringFromTypedValue( response );
                RpcLogger.SERVER_LOG.info( 
                        m_requestDescription
                        + " serviced successfully in " + m_duration
                        + " with response: " 
                        + Platform.NEWLINE
                        + stringReturnValue );
                final String shortStringReturnValue = ( null == stringReturnValue ) ? 
                        " with response: null" 
                        : ( 150 >= stringReturnValue.length() ) ? 
                            " with response: " + stringReturnValue
                            : ".";
                LOG.debug( m_requestDescription + " serviced successfully in " + m_duration 
                          + shortStringReturnValue );
                success = true;
                
                respondWithSuccess( BeanFactory.newBean( RequestInvocationResult.class )
                        .setReturnValue( stringReturnValue ) );
            }
            catch ( final Exception ex )
            {
                handleLocalMethodInvocationFailure( 
                        success,
                        methodInvokeParams,
                        ( null == ex.getCause() || InvocationTargetException.class != ex.getClass() ) ? 
                                ex 
                                : ex.getCause() );
            }
            finally
            {
                work.completed();
            }
        }
        
        
        private void handleLocalMethodInvocationFailure(
                final boolean localMethodInvocationSuccessful,
                final List< Object > methodInvokeParams, 
                final Throwable ex )
        {
            if ( localMethodInvocationSuccessful )
            {
                RpcLogger.SERVER_LOG.warn( "Failed to send response back to client.", ex );
                return;
            }
            
            final String code;
            final int httpResponseCode;
            if ( FailureTypeObservable.class.isAssignableFrom( ex.getClass() ) )
            {
                FailureType type = ((FailureTypeObservable) ex).getFailureType();
                code = type.getCode();
                httpResponseCode = type.getHttpResponseCode();
            }
            else
            {
                code = GenericFailure.INTERNAL_ERROR.getCode();
                httpResponseCode = GenericFailure.INTERNAL_ERROR.getHttpResponseCode();
            }

            final Failure failure = BeanFactory.newBean( Failure.class )
                    .setMessage( ExceptionUtil.getReadableMessage( ex ) )
                    .setCode( code )
                    .setHttpResponseCode( httpResponseCode );            
            
            final Level failureLogLevel = RpcLogger.getLogLevelForHttpResponseCode( httpResponseCode );
            if ( Level.INFO == failureLogLevel )
            {
                failure.setMessage( ex.getMessage() );
            }
            String logMessage = "Servicing " + m_requestDescription + ", FAILURE occurred after " + m_duration
                    + " attempting to execute " + m_rpcResourceMethod
                    + " with parameters "
                    + LogUtil.getShortVersion(Arrays.deepToString(
                    CollectionFactory.toArray(Object.class, methodInvokeParams)))
                    + ".  Failure response: "
                    + Platform.NEWLINE + JsonMarshaler.formatPretty(failure.toJson());

            if ( Level.INFO == failureLogLevel )
            {
                RpcLogger.SERVER_LOG.log( failureLogLevel, logMessage);
                LOG.log( failureLogLevel,
                        "Servicing " + m_requestDescription + ", FAILURE occurred after "
                        + m_duration + ": " 
                        + Platform.NEWLINE + code + " / HTTP " + httpResponseCode
                        + Platform.NEWLINE + ex.getMessage() );
            }
            else
            {
                RpcLogger.SERVER_LOG.log( failureLogLevel, logMessage, ex );
                LOG.log( failureLogLevel,
                        "Servicing " + m_requestDescription + ", FAILURE occurred after "
                        + m_duration + ": " 
                        + Platform.NEWLINE + code + " / HTTP " + httpResponseCode, ex );
            }
            
            respondWithFailure( failure );
        }
        
        
        private void respondWithSuccess( final RequestInvocationResult result )
        {
            final Response response = BeanFactory.newBean( Response.class );
            response.setSuccess( result );
            response.setRequestId( m_request.getId() );
            try
            {
                m_msgSender.send( m_networkMessageEncoder.encode(
                        new JsonNetworkMessage( response.toJson( NamingConventionType.UNDERSCORED ) ) ) );
            }
            catch ( NetworkConnectionClosedException ex )
            {
                RpcLogger.SERVER_LOG.warn( 
                        "Failed to respond to client since the connection was closed.", ex );
            }
        }
        
        
        private void respondWithFailure( final Failure failure )
        {
            final Response response = BeanFactory.newBean( Response.class );
            response.setFailure( failure );
            response.setRequestId( m_request.getId() );
            try
            {
                m_msgSender.send( m_networkMessageEncoder.encode(
                        new JsonNetworkMessage( response.toJson( NamingConventionType.UNDERSCORED ) ) ) );
            }
            catch ( NetworkConnectionClosedException ex )
            {
                RpcLogger.SERVER_LOG.warn( 
                        "Failed to respond to client since the connection was closed.", ex );
            }
        }
        
        private String m_requestDescription;
        private Method m_rpcResourceMethod;
        
        private final Request m_request;
        private final RpcResource m_rpcResource;
        private final NetworkMessageSender m_msgSender;
        private final Duration m_duration = new Duration();
    } // end inner class def
    
    
    synchronized public void unregister( 
            String instanceName, 
            final Class< ? extends RpcResource > rpcResourceApi )
    {
        verifyNotShutdown();

        if ( null == instanceName )
        {
            instanceName = "";
        }
        
        final String type = RpcResourceUtil.getResourceName( rpcResourceApi );
        if ( !m_incomingRequestHandlers.containsKey( type ) 
                || null == m_incomingRequestHandlers.get( type ).remove( instanceName ) )
        {
            throw new IllegalStateException( "Wasn't registered: " + type + "." + instanceName );
        }
    }
    
    
    private final Map< String, Map< String, RpcResource > > m_incomingRequestHandlers = 
            new ConcurrentHashMap<>();
    private final WorkPool m_rpcCallHandlerThreadPoolExecutor =
            WorkPoolFactory.createWorkPool( 64, getClass().getSimpleName() );
    private final JsonNetworkMessageEncoder m_networkMessageEncoder =
            new JsonNetworkMessageEncoder();
    private final TcpIpServer m_server;
    
    private final static Logger LOG = Logger.getLogger( RpcServerImpl.class );
}
