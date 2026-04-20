/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.http;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

public final class ServletHttpRequest implements HttpRequest
{
    public ServletHttpRequest( final HttpServletRequest request )
    {
        m_request = request;
        m_type = RequestType.valueOf( request.getMethod() );
        
        final Map< String, String > headers = new HashMap<>();
        final Map< String, String > queryParams = new HashMap<>();
        
        final Enumeration< String > he = request.getHeaderNames();
        while ( he.hasMoreElements() )
        {
            final String name = he.nextElement();
            final Enumeration<String> values = request.getHeaders( name );
            String value = null;
            while ( null != values && values.hasMoreElements() )
            {
                final String v = values.nextElement();
                if ( null != v )
                {
                    if ( null == value )
                    {
                        value = v;
                    }
                    else {
                        value += "," + v;
                    }
                }
            }

            headers.put( name.toLowerCase(), value );
        }
            
        for ( final Map.Entry< String, String [] > qp : request.getParameterMap().entrySet() )
        {
            if ( 1 < qp.getValue().length )
            {
                LOG.warn( "HTTP query parameter " + qp.getKey() + " has " + qp.getValue().length
                           + " values, but only the first value will be considered." );
            }
            if ( qp.getKey().startsWith( HEADER_AS_QUERY_PARAM_PREFIX ) )
            {
                final String headerName = qp.getKey().substring( HEADER_AS_QUERY_PARAM_PREFIX.length() );
                headers.put( headerName.toLowerCase(), qp.getValue()[ 0 ] );
            }
            else
            {
                if ( null == qp.getValue()[ 0 ] || 0 == qp.getValue()[ 0 ].length() )
                {
                    queryParams.put( qp.getKey(), null );
                }
                else
                {
                    queryParams.put( qp.getKey(), qp.getValue()[ 0 ] );
                }
            }
        }
        
        m_headers = Collections.unmodifiableMap( headers );
        m_queryParams = Collections.unmodifiableMap( queryParams );
    }
    
    
    public RequestType getType()
    {
        return m_type;
    }
    
    
    public String getOriginalClientRequestUrl()
    {
        return ( null == m_request.getRequestURL() ) ? "" : m_request.getRequestURL().toString();
    }
    
    
    public String getFullOriginalClientRequestUrl()
    {
        final String requestUrl = getOriginalClientRequestUrl();
        final String queryString = m_request.getQueryString();

        if ( null == queryString ) 
        {
            return requestUrl;
        } 
        return requestUrl + "?" + queryString;
    }
    
    
    public String getPathInfo()
    {
        return m_request.getPathInfo();
    }
    
    
    public String getHeader( final String name )
    {
        String retval = m_headers.get( name.toLowerCase() );
        if ( null == retval )
        {
            retval = m_request.getHeader( name );
            if ( null != retval )
            {
                LOG.info( "HTTP header " + name 
                          + " was set in a deferred manner and has a value of: " + retval );
            }
        }
        
        return retval;
    }
    
    
    public String getHeader( final HttpHeaderType header )
    {
        return getHeader( header.getHttpHeaderName() );
    }


    public Map< String, String > getHeaders()
    {
        return m_headers;
    }
    
    
    public String getQueryParam( final String name )
    {
        return m_queryParams.get( name );
    }


    public Map< String, String > getQueryParams()
    {
        return m_queryParams;
    }
    
    
    public String getRemoteAddr()
    {
        return m_request.getRemoteAddr();
    }
    
    
    public String getRemoteHost()
    {
        return m_request.getRemoteHost();
    }
    
    
    public int getRemotePort()
    {
        return m_request.getRemotePort();
    }
    
    
    public String getRemoteUser()
    {
        return m_request.getRemoteUser();
    }
    
    
    public ServletInputStream getInputStream() throws IOException
    {
        return m_request.getInputStream();
    }
    
    
    public String getContentType()
    {
        return m_request.getContentType();
    }
    
    
    private final RequestType m_type;
    private final Map< String, String > m_queryParams;
    private final Map< String, String > m_headers;
    private final HttpServletRequest m_request;

    public final static String HEADER_AS_QUERY_PARAM_PREFIX = "http-header-";
    private final static Logger LOG = Logger.getLogger( ServletHttpRequest.class );
}
