/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.dispatch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.ui.ModelMap;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.lang.RuntimeInformationLogger;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.server.HttpLogger;
import com.spectralogic.s3.server.WireLogger;
import com.spectralogic.s3.server.authorization.AuthorizationValidationStrategyImpl;
import com.spectralogic.s3.server.authorization.api.AuthorizationValidationStrategy;
import com.spectralogic.s3.server.domain.HttpErrorResultApiBean;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.frmwrk.RequestProcessingThreadRenamer;
import com.spectralogic.s3.server.handler.command.CommandExecutionParamsImpl;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.find.RequestHandlerProvider;
import com.spectralogic.s3.server.handler.reqhandler.RequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.system.GetRequestHandlersRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.BaseServlet;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.postgres.PostgresDataManager;
import com.spectralogic.util.db.service.BeansServiceManagerImpl;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.FailureTypeObservable;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.exception.RetryObservable;
import com.spectralogic.util.healthmon.CpuHogDetector;
import com.spectralogic.util.healthmon.CpuHogListenerImpl;
import com.spectralogic.util.healthmon.DeadlockDetector;
import com.spectralogic.util.healthmon.DeadlockListenerImpl;
import com.spectralogic.util.healthmon.MemoryHogDetector;
import com.spectralogic.util.healthmon.MemoryHogListenerImpl;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.client.RpcRequestUnserviceableException;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.notification.dispatch.bean.HttpNotificationEventDispatcher;
import com.spectralogic.util.security.ClientAttackMitigator;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public class RequestDispatcherImpl implements RequestDispatcher
{
    public RequestDispatcherImpl(
            final DataSource dataSource,
            final RpcClient rpcClient )
    {
        this( createServiceManager( dataSource ), rpcClient );
    }
    
    
    private static BeansServiceManager createServiceManager( final DataSource dataSource )
    {
        final DataManager dataManager = new PostgresDataManager(
                16,
                CollectionFactory.toSet( DaoDomainsSeed.class ) );
        dataManager.setDataSource( dataSource );
        final WorkPool notificationEventDispatcherWp = 
                WorkPoolFactory.createWorkPool( 4, NotificationEventDispatcher.class.getSimpleName() );
        final NotificationEventDispatcher notificationEventDispatcher =
                new HttpNotificationEventDispatcher( notificationEventDispatcherWp );
        final BeansServiceManager serviceManager = BeansServiceManagerImpl.create(
                notificationEventDispatcher, 
                dataManager,
                CollectionFactory.toSet( DaoServicesSeed.class ) );
        return serviceManager;
    }
    
    
    public RequestDispatcherImpl(
            final BeansServiceManager serviceManager,
            final RpcClient rpcClient )
    {
        this( serviceManager, 
              rpcClient.getRpcResource( 
                      DataPlannerResource.class, 
                      null, 
                      ConcurrentRequestExecutionPolicy.CONCURRENT ),
              rpcClient.getRpcResource( 
                      DataPolicyManagementResource.class, 
                      null, 
                      ConcurrentRequestExecutionPolicy.CONCURRENT ),
              rpcClient.getRpcResource( 
                      TapeManagementResource.class, 
                      null, 
                      ConcurrentRequestExecutionPolicy.CONCURRENT ),
              rpcClient.getRpcResource( 
                      PoolManagementResource.class, 
                      null, 
                      ConcurrentRequestExecutionPolicy.CONCURRENT ),
              rpcClient.getRpcResource( 
                      TargetManagementResource.class, 
                      null, 
                      ConcurrentRequestExecutionPolicy.CONCURRENT ) );
    }
    
    
    public RequestDispatcherImpl(
            final BeansServiceManager serviceManager, 
            final DataPlannerResource plannerResource,
            final DataPolicyManagementResource dataPolicyResource,
            final TapeManagementResource tapeResource,
            final PoolManagementResource poolResource,
            final TargetManagementResource targetResource )
    {
        RuntimeInformationLogger.logRuntimeInformation();
        m_plannerResource = plannerResource;
        m_dataPolicyResource = dataPolicyResource;
        m_tapeResource = tapeResource;
        m_poolResource = poolResource;
        m_targetResource = targetResource;
        m_serviceManager = serviceManager;
        m_authorizationValidationStrategy = new AuthorizationValidationStrategyImpl(
                m_serviceManager.getRetriever( User.class ) );
    }
    

    public ServletResponseStrategy handleS3Request( 
            final RequestProcessingThreadRenamer threadRenamer,
            final DS3Request request,
            final ModelMap model )
    {
        /*
         * Check to see if this request should be dropped on the floor due to DoS detection
         */
        s_lastRequestHandlerDispatchedTo = null;
        final CommandExecutionParams params = new CommandExecutionParamsImpl(
                m_serviceManager, 
                m_plannerResource, 
                m_dataPolicyResource,
                m_tapeResource, 
                m_poolResource,
                m_targetResource,
                request, 
                model );
        if ( DOS_MITIGATOR.isAttackFromClient( request.getHttpRequest() )
                || DOS_MITIGATOR.isAttackOnUser( request.getAuthorization().getId() ) )
        {
            WireLogger.LOG.warn( "Dropped request on floor from '" 
                      + request.getHttpRequest().getRemoteHost()
                      + "', address '" 
                      + request.getHttpRequest().getRemoteAddr() 
                      + "', port '"
                      + request.getHttpRequest().getRemotePort() 
                      + "', user '" 
                      + request.getHttpRequest().getRemoteUser()
                      + "' since a dictionary attack on a user's password " 
                      + "or a denial of service attack may be in progress." );
            request.getHttpResponse().setStatus( HttpServletResponse.SC_SERVICE_UNAVAILABLE );
            
            final HttpErrorResultApiBean errorResult = BeanFactory.newBean( HttpErrorResultApiBean.class )
                    .setMessage( "Too many failed authentication attempts.  Retry in a few minutes." )
                    .setCode( GenericFailure.FORBIDDEN.getCode() )
                    .setHttpErrorCode( GenericFailure.FORBIDDEN.getHttpResponseCode() );
            return BeanServlet.serviceRequest(
                    params, 
                    HttpServletResponse.SC_FORBIDDEN, errorResult );
        }
        
        /*
         * Dispatch the request
         */
        try
        {
            try
            {
                final RequestHandler requestHandler = getRequestHandler( request );
                s_lastRequestHandlerDispatchedTo = requestHandler.getClass();
                final MonitoredWork monitoredWork = new MonitoredWork( 
                        StackTraceLogging.SHORT, requestHandler.getClass().getSimpleName() );
                try
                {
                    final String incomingRequestMessage = 
                            request.getRequestReceivedMessage( requestHandler.getClass().getSimpleName() );
                    LOG.info( incomingRequestMessage );
                    WireLogger.LOG.info( incomingRequestMessage );
                    threadRenamer.run();
                    
                    String nar = String.valueOf( BaseServlet.NUM_ACTIVE_REQUESTS.get() );
                    while ( 3 > nar.length() )
                    {
                        nar = " " + nar;
                    }
                    final char reqSrcCode = "127.0.0.1".equals( request.getHttpRequest().getRemoteAddr() ) ?
                            'L'
                            : ( ( null == request.getHttpRequest().getHeader( 
                                    S3HeaderType.REPLICATION_SOURCE_IDENTIFIER ) ) ? 'E' : 'R' );
                    HttpLogger.LOG.info( 
                            " " + reqSrcCode + "  |"
                            + nar + " | " + requestHandler.getClass().getSimpleName() );
                    
                    request.getRestRequest().validate();
                    request.getAuthorization().validate( m_authorizationValidationStrategy );
    
                    LOG.info( "Dispatching request to " + requestHandler.getClass().getSimpleName() + "." );
                    model.addAttribute(
                            BaseServlet.PROCESSING_REQUEST_HANDLER,
                            requestHandler.getClass().getSimpleName() );
                    request.getHttpResponse().addHeader( 
                            "RequestHandler-Version",
                            m_requestHandlerVersions.get( requestHandler.getClass() ) );
                    return requestHandler.handleRequest( params );
                }
                finally
                {
                    monitoredWork.completed();
                }
            }
            catch ( final RuntimeException ex )
            {
                Throwable t = ex;
                do
                {
                    if ( RpcRequestUnserviceableException.class.isAssignableFrom( ex.getClass() ) )
                    {
                        throw new FailureTypeObservableException( 
                                GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                                "Underlying software components are still initializing or offline.  " 
                                + "Try again later.",
                                ex );
                    }
                    if ( FailureTypeObservableException.class.isAssignableFrom( t.getClass() ) )
                    {
                        throw (FailureTypeObservableException)t;
                    }
                    t = t.getCause();
                } while ( null != t );
                throw new S3RestException( ex );
            }
        }
        catch ( final RuntimeException ex )
        {
            final FailureType failureType = ( (FailureTypeObservable)ex ).getFailureType();
            final HttpErrorResultApiBean errorResult = BeanFactory.newBean( HttpErrorResultApiBean.class );
            errorResult.setCode( failureType.getCode() );
            errorResult.setHttpErrorCode( failureType.getHttpResponseCode() );
            if ( HttpServletResponse.SC_FORBIDDEN == failureType.getHttpResponseCode() )
            {
                DOS_MITIGATOR.authorizationFailed( 
                        request.getAuthorization().getId(),
                        request.getHttpRequest() );
            }
            if ( HttpServletResponse.SC_INTERNAL_SERVER_ERROR == failureType.getHttpResponseCode() )
            {
                LOG.warn(
                      "Exceptional failure occurred (" + failureType + " / " 
                      + failureType.getHttpResponseCode() + ").", ex );
            }
            else
            {
                LOG.info( ExceptionUtil.getMessageWithSingleLineStackTrace(
                        "Request failed (" + failureType + " / " + failureType.getHttpResponseCode() + ").", 
                        ex ) );
            }
            
            errorResult.setMessage( ExceptionUtil.getReadableMessage( ex ) );
            errorResult.setResource( request.getRequestPath() );
            errorResult.setResourceId( request.getRequestId() );
            
            final HttpServletResponse resp = request.getHttpResponse();
            if ( failureType.getHttpResponseCode() == HttpServletResponse.SC_TEMPORARY_REDIRECT )
            {
                try
                {
                    if ( s_sleepsFor307sEnabled )
                    {
                        Thread.sleep( extractRetryAfterInSeconds( ex ) * 1000 );
                    }
                    resp.setHeader( "Location", request.getHttpRequest().getFullOriginalClientRequestUrl() );
                }
                catch ( final InterruptedException e) 
                {
                    LOG.warn( "Failed to send redirect message.", e );
                }
            }
            else if ( failureType.getHttpResponseCode() == HttpServletResponse.SC_SERVICE_UNAVAILABLE )
            {
                resp.addHeader( "Retry-After", Integer.toString( extractRetryAfterInSeconds( ex ) ) );
            }
            
            return BeanServlet.serviceRequest(
                    params, 
                    failureType.getHttpResponseCode(),
                    errorResult );
        }
    }
    
    
    private int extractRetryAfterInSeconds( final Throwable t )
    {
        if ( RetryObservable.class.isAssignableFrom( t.getClass() ) )
        {
            return ( (RetryObservable)t ).getRetryAfterInSeconds();
        }
        
        LOG.info( "Using default retry after 30 seconds since " + t.getClass() 
                  + " is not " + RetryObservable.class.getSimpleName() + "." );
        return 30;
    }
    
    
    private RequestHandler getRequestHandler( final DS3Request request )
    {
        final Set< RequestHandler > acceptableHandlers = new HashSet<>();
        final Map< String, String > closeHandlers = new HashMap<>();
        for ( final RequestHandler handler : RequestHandlerProvider.getAllRequestHandlers() )
        {
            final String handleFailure =
                    handler.getCanHandleRequestDeterminer().getFailureToHandle( request );
            if ( null == handleFailure )
            {
                acceptableHandlers.add( handler );
            }
            else if ( !handleFailure.isEmpty() )
            {
                closeHandlers.put( handler.getClass().getName(), handleFailure );
            }
        }
        
        if ( acceptableHandlers.size() > 1 )
        {
            LOG.info( request.getRequestReceivedMessage( null ) );
            throw new RuntimeException( 
                    "Multiple candidate handlers found for request: " + acceptableHandlers );
        }
        
        if ( acceptableHandlers.isEmpty() )
        {
            LOG.info( request.getRequestReceivedMessage( null ) );
            if ( closeHandlers.isEmpty() )
            {
                throw new S3RestException( 
                        AWSFailure.INVALID_URI,
                        "Request was malformed: No handlers exist that can handle the request." );
            }
            throw new S3RestException( 
                    AWSFailure.INVALID_URI,
                    "Request was malformed: No handlers exist that can handle the request.  " 
                    + "It looks like you meant to use one of these handlers: " + closeHandlers );
        }
        
        if ( request.getHttpRequest().getFullOriginalClientRequestUrl().contains( ";" ) )
        {
            LOG.info( request.getRequestReceivedMessage( null ) );
            throw new S3RestException( 
                    AWSFailure.INVALID_URI,
                    "Request contained an unescaped semicolon." );
        }
        
        return acceptableHandlers.iterator().next();
    }
    
    
    private static void startHealthMonitoring()
    {
        new DeadlockDetector( 30000 ).addDeadlockListener( 
                new DeadlockListenerImpl() );
        new MemoryHogDetector( 30000 ).addMemoryHogListener(
                new MemoryHogListenerImpl( 0.6f, 0.8f ) );
        new CpuHogDetector( 5000, 4000 ).addCpuHogListener( 
                new CpuHogListenerImpl() );
    }
    
    
    public static Class< ? > getLastRequestHandlerDispatchedTo()
    {
        return s_lastRequestHandlerDispatchedTo;
    }
    
    
    public static void disableSleepsFor307s()
    {
        s_sleepsFor307sEnabled = false;
    }
    
    
    private final DataPlannerResource m_plannerResource;
    private final DataPolicyManagementResource m_dataPolicyResource;
    private final TapeManagementResource m_tapeResource;
    private final PoolManagementResource m_poolResource;
    private final TargetManagementResource m_targetResource;
    private final BeansServiceManager m_serviceManager;
    private final AuthorizationValidationStrategy m_authorizationValidationStrategy;
    private final Map< Class< ? >, String > m_requestHandlerVersions = 
            GetRequestHandlersRequestHandler.getRequestHandlerVersions();
    
    private static volatile boolean s_sleepsFor307sEnabled = true;
    private static volatile Class< ? > s_lastRequestHandlerDispatchedTo;
    private static final Logger LOG = Logger.getLogger(RequestDispatcherImpl.class);
    private final static ClientAttackMitigator DOS_MITIGATOR = new ClientAttackMitigator(); 
    static
    {
        startHealthMonitoring();
    }
}
