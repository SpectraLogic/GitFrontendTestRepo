package com.spectralogic.s3.server.dispatch;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.HttpLogger;
import com.spectralogic.s3.server.WireLogger;
import com.spectralogic.s3.server.frmwrk.RequestProcessingThreadRenamer;
import com.spectralogic.s3.server.request.DS3RequestImpl;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.BaseServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.log.LogUtil;

@Controller
public class S3Controller
{
    public S3Controller( final RequestDispatcher requestDispatcher )
    {
        m_requestDispatcher = requestDispatcher;
        
        final String msg = "Ready to Process Incoming Requests";
        LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( msg ) );
        WireLogger.LOG.info( LogUtil.getLogMessageImportantHeaderBlock( msg ) );
        HttpLogger.LOG.info( LogUtil.getLogMessageImportantHeaderBlock( msg ) );
    }
    

    /**
     * Handles all incoming web requests, whether they're GETs, PUTs, etc.
     */
    @RequestMapping(value="/**")
    public String handleS3Request(
            final HttpServletRequest httpRequest,
            final HttpServletResponse httpResponse,
            final ModelMap model )
    {
        if ( m_shutdown )
        {
            throw new IllegalStateException( "Controller has been shutdown." );
        }
        
        BaseServlet.NUM_ACTIVE_REQUESTS.incrementAndGet();
        final Duration duration = new Duration();
        final DS3Request request = new DS3RequestImpl( httpRequest, httpResponse );
        final RequestProcessingThreadRenamer threadRenamer = 
                new RequestProcessingThreadRenamer( request.getRequestId() );
        try
        {
            model.put( BaseServlet.PROCESSING_TIME_DURATION_KEY, duration );
            final ServletResponseStrategy retval = 
                    m_requestDispatcher.handleS3Request( threadRenamer, request, model );
            request.getHttpResponse().addHeader(
                    S3HeaderType.AMAZON_REQUEST_ID.getHttpHeaderName(),
                    String.valueOf( request.getRequestId() ) );
            LOG.info( request.getRequestProcessedMessage( retval ) );
            return retval.getServletNameToProvideResponseWith();
        }
        finally
        {
            BaseServlet.NUM_ACTIVE_REQUESTS.decrementAndGet();
            threadRenamer.shutdown();
        }
    }
    
    
    public void shutdown()
    {
        final String msg = "Shutdown";
        LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( msg ) );
        WireLogger.LOG.info( LogUtil.getLogMessageImportantHeaderBlock( msg ) );
        HttpLogger.LOG.info( LogUtil.getLogMessageImportantHeaderBlock( msg ) );
        
        m_shutdown = true;
    }
    
    
    private static final Logger LOG = Logger.getLogger( S3Controller.class );
    private final RequestDispatcher m_requestDispatcher;
    private volatile boolean m_shutdown = false;
}