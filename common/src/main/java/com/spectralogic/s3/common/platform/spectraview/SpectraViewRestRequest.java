/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.spectraview;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.util.http.HttpUtil;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

/**
 * A RESTful request to be made against SpectraView (The Ruby on Rails GUI server software component) locally.
 */
public final class SpectraViewRestRequest
{
    public SpectraViewRestRequest( 
            final RequestType requestType, 
            final String requestPath,
            final Level logLevel )
    {
        m_requestType = requestType;
        m_requestPath = requestPath;
        m_logLevel = logLevel;
        Validations.verifyNotNull( "Request type", m_requestType );
        Validations.verifyNotNull( "Request path", m_requestPath );
        Validations.verifyNotNull( "Log level", m_logLevel );
        m_headers.put( "Spectra-Client-Id", "9b7311ec-9bf3-11e4-bc21-001517e93068" );
    }
    
    
    synchronized public SpectraViewRestRequest addHeader( final String key, final String value )
    {
        m_headers.put( key, value );
        return this;
    }
    
    
    /**
     * For testing purposes only.
     */
    public static void setOverrideRunReturnValue( final String value )
    {
        s_overrideRunner = new SpectraViewRestRequestOverrideRunner()
        {
            public String run()
            {
                return value;
            }
        };
    }
    
    
    public static void setOverrideRunner( final SpectraViewRestRequestOverrideRunner r )
    {
        s_overrideRunner = r;
    }
    
    
    public interface SpectraViewRestRequestOverrideRunner
    {
        String run();
    } // end inner class def
    
    
    synchronized public String run()
    {
        if ( null != s_overrideRunner )
        {
            final SpectraViewRestRequestOverrideRunner retval = s_overrideRunner;
            s_overrideRunner = null;
            return retval.run();
        }
        
        final Duration duration = new Duration();
        HttpURLConnection connection = null;
        final MonitoredWork work = new MonitoredWork(
                StackTraceLogging.SHORT,
                "RESTful request on " + m_requestPath );
        final String logMessage = "request to spectra view (" + m_requestType + " " + m_requestPath 
                + " with headers " + m_headers + ")";
        try
        {
            final URL url = new URL( "https://localhost/api/" + m_requestPath );
            connection = (HttpURLConnection)url.openConnection();
            HttpUtil.hackConnectionForBadSslCertificate( connection );
            
            connection.setRequestMethod( m_requestType.toString() );
            connection.setDoOutput( true );
            connection.setAllowUserInteraction( false );
            for ( final Map.Entry< String, String > h : m_headers.entrySet() )
            {
                connection.setRequestProperty( h.getKey(), h.getValue() );
            }
            LOG.log( m_logLevel, "Making " + logMessage + "..." );
            connection.connect();

            return IOUtils.toString( connection.getInputStream(), "UTF-8" );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            work.completed();
            if ( null != connection )
            {
                try
                {
                    final int code = connection.getResponseCode();
                    if ( 200 > code || 300 <= code )
                    {
                        LOG.warn( "Failed " + logMessage + " with status code "
                                  + connection.getResponseCode() + " after " + duration + "." );
                    }
                    else 
                    {
                        LOG.log( m_logLevel, "Finished " + logMessage + "." );
                    }
                    connection.getInputStream().close();
                }
                catch ( final IOException ex )
                {
                    LOG.warn( "Failed to close URL connection.", ex );
                }
            }
        }
    }
    
    
    private final RequestType m_requestType;
    private final String m_requestPath;
    private final Map< String, String > m_headers = new HashMap<>();
    
    private static volatile SpectraViewRestRequestOverrideRunner s_overrideRunner;
    private final static Logger LOG = Logger.getLogger( SpectraViewRestRequest.class );
    private final Level m_logLevel;
}
