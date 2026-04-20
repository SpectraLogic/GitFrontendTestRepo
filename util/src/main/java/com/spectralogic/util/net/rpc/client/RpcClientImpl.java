/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.log.ThrottledLog;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.domain.Request;
import com.spectralogic.util.net.rpc.domain.Response;
import com.spectralogic.util.net.rpc.domain.RpcFrameworkErrorCode;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcLogger;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceUtil;
import com.spectralogic.util.net.tcpip.TcpIpClient;
import com.spectralogic.util.net.tcpip.TcpIpClientImpl;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageSender;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessage;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageEncoder;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;
import com.spectralogic.util.shutdown.StandardShutdownListener;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public class RpcClientImpl extends BaseShutdownable implements RpcClient
{
    public RpcClientImpl( final String host, final int port )
    {
        Validations.verifyNotNull( "Host", host );
        Validations.verifyInRange( "Port", 1, 65535, port );
        m_host = host;
        m_port = port;
        
        for ( int i = 0; i < DISPATCH_THREAD_COUNT; ++i )
        {
            m_responseDispatchWorkPool.submit( m_responseDispatcher );
        }
        addShutdownListener( new CleanupOnShutdown() );
        
        RpcLogger.CLIENT_LOG.info( LogUtil.getLogMessageImportantHeaderBlock( "Ready to send requests" ) );
    }
    
    
    private final class CleanupOnShutdown extends CriticalShutdownListener
    {
        public void shutdownOccurred()
        {
            synchronized ( m_clientLock )
            {
                if ( null != m_client )
                {
                    if ( !m_client.isShutdown() )
                    {
                        m_client.shutdown();
                    }
                }
            }
        }
    } // end inner class def 
    
    
    public < T extends RpcResource > T getRpcResource( 
            final Class< T > rpcResourceApi, 
            final String resourceInstanceName,
            final ConcurrentRequestExecutionPolicy concurrentRequestExecutionPolicy )
    {
        return getRpcResource( 
                rpcResourceApi,
                resourceInstanceName,
                concurrentRequestExecutionPolicy,
                DEFAULT_SECONDS_TO_WAIT_FOR_RPC_SERVER_TO_COME_ONLINE * 1000 );
    }
    
    
    public < T extends RpcResource > T getRpcResource(
            final Class< T > rpcResourceApi, 
            final String resourceInstanceName,
            final ConcurrentRequestExecutionPolicy concurrentRequestExecutionPolicy,
            final int maxDelayInMillisForResourceInstanceToComeOnline )
    {
        verifyNotShutdown();
        RpcResourceUtil.validate( rpcResourceApi );
        
        InvocationHandler ih = new RpcResourceClientStubInvocationHandler( 
                RpcResourceUtil.getResourceName( rpcResourceApi ), 
                resourceInstanceName,
                this,
                maxDelayInMillisForResourceInstanceToComeOnline,
                concurrentRequestExecutionPolicy );
        if ( RpcClientInvocationDiagnostics.getInstance().isEnabled() )
        {
            final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ih );
            ih = btih;
            RpcClientInvocationDiagnostics.getInstance().registerBtih(
                    rpcResourceApi, resourceInstanceName, btih );
        }
        
        return InterfaceProxyFactory.getProxy( rpcResourceApi, ih );
    }
    
    
    public RpcFuture< ? > invokeRemoteProcedureCall( 
            String resourceTypeName,
            final String resourceInstanceName,
            String methodName,
            final Class< ? > methodReturnType,
            final RpcMethodNullReturn rpcMethodNullReturn,
            final List< Object > methodArgs )
    {
        verifyNotShutdown();
        reshapeMethodArgsForVariableLengthFinalParameter( methodArgs );
        for ( final Object arg : methodArgs )
        {
            RpcResourceUtil.validateResponse( arg, GenericFailure.BAD_REQUEST );
        }
        
        resourceTypeName = 
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( resourceTypeName );
        methodName = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( methodName );
        
        while ( 250 < getNumberOfOutstandingRequests() )
        {
            TOO_MANY_REQUESTS_LOG.warn( 
                    "There are " + getNumberOfOutstandingRequests()
                    + " outstanding requests.  New RPC calls will be blocked for a second " 
                    + "for the queue of outstanding work to reduce." );
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        final long requestId = m_nextRequestId.getAndIncrement();
        String requestDescription = "<" + requestId + ">";
        try
        {
            final TcpIpClient< JsonNetworkMessage > client = getClient();
            final Duration durationToMarshal = new Duration();
            final Request request = BeanFactory.newBean( Request.class );
            request.setId( requestId );
            request.setType( resourceTypeName );
            request.setInstance( resourceInstanceName );
            request.setMethod( methodName );
            
            /*
             * The reason that we set a placeholder and then replace the placeholder later is that this is FAR
             * more performant when we marshal the JSON i.e. if we set the request params, which could be VERY
             * lengthy, onto the bean and then marshal the bean, we will have processed the request params
             * payload twice, and the second time we process it will be far more expensive than the first.
             */
            request.setParams( REQUEST_PARAMS_PLACEHOLDER );
            
            requestDescription = RpcLogger.getDescriptionForRequest( 
                    request.getId(), resourceTypeName, resourceInstanceName, methodName );
            
            final ClientRpcFuture future = new ClientRpcFuture(
                    methodReturnType, rpcMethodNullReturn, request.getId(), requestDescription );
            m_outstandingRequests.put( Long.valueOf( request.getId() ), future );
            
            String jsonRequest =
                    request.toJson( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE );
            jsonRequest = jsonRequest.replace( 
                    "\"" + REQUEST_PARAMS_PLACEHOLDER + "\"", 
                    JsonMarshaler.marshal(
                            methodArgs, 
                            NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ) );
            RpcLogger.CLIENT_LOG.info( 
                    "Sending " + requestDescription + " [" + durationToMarshal + "]:" 
                    + Platform.NEWLINE 
                    + LogUtil.getShortVersion( LogUtil.hideSecretsInJson(
                            JsonMarshaler.formatPretty( ( jsonRequest ) ) ) ) );
            LOG.info( requestDescription + " sent." );
            
            final Duration durationToTransmit = new Duration();
            client.send( new JsonNetworkMessage( jsonRequest ) );
            RpcLogger.CLIENT_LOG.info( 
                    "Transmitted " + requestDescription + "[" + durationToTransmit + "]" );
            return future;
        }
        catch ( final Exception e )
        {
            RpcLogger.CLIENT_LOG.warn( "Failed to transmit " + requestDescription + ".", e );
            final RpcRequestUnserviceableException ex = new RpcRequestUnserviceableException( 
                    "It is not possible to service RPC request " + resourceTypeName + "." + methodName 
                    + " at this time.", e );
            final ClientRpcFuture future = m_outstandingRequests.remove( Long.valueOf( requestId ) );
            if ( null != future )
            {
                future.completedWithFailure( ex );
            }
            
            synchronized ( m_clientLock )
            {
                if ( null != m_client )
                {
                    if ( !m_client.isShutdown() )
                    {
                        m_client.shutdown();
                    }
                    m_client = null;
                    m_durationSinceLastClientFailure = new Duration();
                }
            }
            
            throw ex;
        }
    }
    
    
    private TcpIpClient< JsonNetworkMessage > getClient()
    {
        if ( null != m_durationSinceLastClientFailure 
                && 0 == m_durationSinceLastClientFailure.getElapsedSeconds() )
        {
            RECENT_CLIENT_CONNECTIVITY_FAILURE_LOG.warn( 
                    "Attempted to get the RPC client too frequently after an RPC connectivity failure.  " 
                    + "This request will wait a second before we try to execute it." );
            try
            {
                Thread.sleep( 1000 - m_durationSinceLastClientFailure.getElapsedMillis() );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        synchronized ( m_clientLock )
        {
            if ( null != m_client )
            {
                return m_client;
            }
            
            m_client = new TcpIpClientImpl<>( 
                    m_host, 
                    m_port, 
                    new ResponseHandler(), 
                    new JsonNetworkMessageDecoder(), 
                    new JsonNetworkMessageEncoder(), 
                    RpcLogger.CLIENT_LOG );
            m_client.addShutdownListener( m_tcpIpClientClosedListener );
            m_client.run();
            return m_client;
        }
    }
    
    
    private int getNumberOfOutstandingRequests()
    {
        return m_outstandingRequests.size();
    }
    
    
    /**
     * Only the last parameter can be of variable length
     */
    private void reshapeMethodArgsForVariableLengthFinalParameter( final List< Object > methodArgs )
    {
        if ( !methodArgs.isEmpty() )
        {
            final Object lastMethodArg = methodArgs.get( methodArgs.size() - 1 );
            if ( null != lastMethodArg && lastMethodArg.getClass().isArray() )
            {
                methodArgs.remove( methodArgs.get( methodArgs.size() - 1 ) );
                for ( int i = 0; i < Array.getLength( lastMethodArg ); ++i )
                {
                    methodArgs.add( Array.get( lastMethodArg, i ) );
                }
            }
        }
    }
    
    
    private final class ResponseHandler implements NetworkMessageHandler< JsonNetworkMessage >
    {
        public void handle(
                final JsonNetworkMessage message, 
                final NetworkMessageSender networkMessageSender )
        {
            try
            {
                m_responseDispatcher.m_responses.put( message );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }
    } // end inner class def
    
    
    private final class RpcResponseDispatcher implements Runnable
    {
        public void run()
        {
            while ( true )
            {
                final JsonNetworkMessage message;
                try
                {
                    message = m_responses.take();
                }
                catch ( final InterruptedException ex )
                {
                    RpcLogger.CLIENT_LOG.debug( "Interrupted.", ex );
                    return;
                }
                
                try
                {
                    RpcLogger.CLIENT_LOG.info( 
                            "RPC Response Received: " + Platform.NEWLINE 
                            + LogUtil.getShortVersion( JsonMarshaler.formatPretty( message.getJson() ) ) );
                    processResponse( JsonMarshaler.unmarshal( Response.class, message.getJson() ) );
                }
                catch ( final RuntimeException ex )
                {
                    RpcLogger.CLIENT_LOG.error( "Failed to process response.", ex );
                }
            }
        }
        
        private final BlockingQueue< JsonNetworkMessage > m_responses =
                new ArrayBlockingQueue<>( DISPATCH_THREAD_COUNT * 8 );
    } // end inner class def
    
    
    private void processResponse( final Response response )
    {
        if ( null != response.getFailure() && null != response.getSuccess() )
        {
            throw new IllegalStateException( "The response indicated both success and failure." );
        }
        if ( null == response.getFailure() && null == response.getSuccess() )
        {
            throw new IllegalStateException( "The response did not indicate success or failure." );
        }
        
        if ( null == response.getSuccess() )
        {
            processFailure( response );
        }
        else
        {
            processSuccess( response );
        }
    }
    
    
    private void processSuccess( final Response response )
    {
        final ClientRpcFuture future = 
                m_outstandingRequests.remove( Long.valueOf( response.getRequestId() ) );
        if ( null == future )
        {
            throw new IllegalStateException( 
                    "Received a response for a request that is not outstanding: " + response.getRequestId() );
        }
        
        RpcClientUtil.logSuccess( LOG, future, response.getSuccess().getReturnValue() );
        
        try
        {
            future.completedWithResponse( response.getSuccess().getReturnValue() );
        }
        finally
        {
            m_globalRpcCompletedEventDispatcher.dispatchRpcCompletedEvent( future );
        }
    }
    
    
    private void processFailure( final Response response )
    {
        final ClientRpcFuture future = 
                m_outstandingRequests.remove( Long.valueOf( response.getRequestId() ) );
        if ( null == future )
        {
            throw new IllegalStateException( 
                    "Received a response for a request that is not outstanding: " + response.getRequestId() );
        }
        
        response.getFailure().setCode( 
                NamingConventionType.CONSTANT.convert( response.getFailure().getCode() ) );
        RpcClientUtil.logFailure(
                LOG,
                future,
                response.getFailure().getMessage(),
                response.getFailure().getCode(), 
                response.getFailure().getHttpResponseCode() );
        
        final RuntimeException ex;
        if ( RpcFrameworkErrorCode.RESOURCE_INSTANCE_NOT_FOUND.toString().equals( 
                response.getFailure().getCode() )
                || RpcFrameworkErrorCode.RESOURCE_TYPE_NOT_FOUND.toString().equals( 
                        response.getFailure().getCode() ) )
        {
            ex = new RpcRequestUnserviceableException( 
                    future.getRequestDescription() + " is not serviceable: " 
                    + response.getFailure().getMessage(), 
                    new RpcProxyException( future.getRequestDescription(), response.getFailure() ) );
        }
        else
        {
            ex = new RpcProxyException( future.getRequestDescription(), response.getFailure() );
        }
        
        try
        {
            future.completedWithFailure( ex );
        }
        finally
        {
            m_globalRpcCompletedEventDispatcher.dispatchRpcCompletedEvent( future );
        }
    }


    public void addRpcCompletedListener( final RpcCompletedListener< Object > listener )
    {
        verifyNotShutdown();
        m_globalRpcCompletedEventDispatcher.addListener( listener );
    }
    
    
    private final class TcpIpClientClosedListener extends StandardShutdownListener
    {
        public void shutdownOccurred()
        {
            final Map< Long, ClientRpcFuture > outstandingRequests;
            outstandingRequests = new HashMap<>( m_outstandingRequests );
            m_outstandingRequests.clear();
            
            for ( final ClientRpcFuture f : outstandingRequests.values() )
            {
                final String msg = 
                        "The TCP/IP connection with the RPC server was closed after " 
                        + f.getRequestDescription() + " was sent.";
                RpcLogger.CLIENT_LOG.warn( msg );
                f.completedWithFailure( new RpcRequestUnserviceableException( msg, null ) );
            }
        }
    } // end inner class def
    
    
    private TcpIpClient< JsonNetworkMessage > m_client;
    private volatile Duration m_durationSinceLastClientFailure;
    private final Object m_clientLock = new Object();
    private final AtomicLong m_nextRequestId = new AtomicLong( 1 );
    
    private final String m_host;
    private final int m_port;
    private final Map< Long, ClientRpcFuture > m_outstandingRequests = new ConcurrentHashMap<>();
    private final GlobalRpcCompletedEventDispatcher m_globalRpcCompletedEventDispatcher =
            new GlobalRpcCompletedEventDispatcher();
    
    private final RpcResponseDispatcher m_responseDispatcher = new RpcResponseDispatcher();
    private final TcpIpClientClosedListener m_tcpIpClientClosedListener =
            new TcpIpClientClosedListener();
    private final WorkPool m_responseDispatchWorkPool = WorkPoolFactory.createWorkPool(
            DISPATCH_THREAD_COUNT,
            RpcResponseDispatcher.class.getSimpleName() );
    
    private final static ThrottledLog TOO_MANY_REQUESTS_LOG = 
            new ThrottledLog( RpcLogger.CLIENT_LOG, 10000 );
    private final static ThrottledLog RECENT_CLIENT_CONNECTIVITY_FAILURE_LOG = 
            new ThrottledLog( RpcLogger.CLIENT_LOG, 10000 );
    private final static Logger LOG = Logger.getLogger( RpcClientImpl.class );
    private final static int DEFAULT_SECONDS_TO_WAIT_FOR_RPC_SERVER_TO_COME_ONLINE = 300;
    
    /*
     * Do not change this without careful consideration.  The single dispatch thread means that we are
     * guaranteed that responses will be (i) processed in the order received, and (ii) that the processing
     * of response B cannot begin until the processing of response A has completed (or at least until the
     * listeners added return).  This is a simplistic design that eliminates potential concurrency issues
     * in clients of this framework.
     */
    private final static int DISPATCH_THREAD_COUNT = 1;
    private final static String REQUEST_PARAMS_PLACEHOLDER = 
            ( RpcClientImpl.class.getName() + ".RequestParamsPlaceholder" ).replace( ".", "___" );
}
