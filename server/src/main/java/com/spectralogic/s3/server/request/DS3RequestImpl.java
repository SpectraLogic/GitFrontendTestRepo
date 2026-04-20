package com.spectralogic.s3.server.request;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.authorization.S3AuthorizationImpl;
import com.spectralogic.s3.server.authorization.api.S3Authorization;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.api.RequestParameterValue;
import com.spectralogic.s3.server.request.rest.RestRequest;
import com.spectralogic.s3.server.request.rest.RestRequestImpl;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.http.ServletHttpRequest;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;

public final class DS3RequestImpl implements DS3Request
{
    public DS3RequestImpl(
            final HttpServletRequest request, 
            final HttpServletResponse response )
    {
        Validations.verifyNotNull( "Request", request );
        Validations.verifyNotNull( "Response", response );
        
        m_httpRequest = new ServletHttpRequest( request );
        m_httpResponse = response;
        m_requestId = getNextRequestId();
        m_authorization = new S3AuthorizationImpl( m_httpRequest );
        String pathInfo; 
        try
        {
            //NOTE: we do this ourselves since tomcat incorrectly escapes %5C as '/' instead of '\'
            pathInfo = getPathInfoFromOriginalUrl( m_httpRequest.getOriginalClientRequestUrl() );
        }
        catch ( MalformedURLException | DecoderException e )
        {
            LOG.warn("Error parsing URI - retrying with Tomcat built-in escaping.", e);
            pathInfo = m_httpRequest.getPathInfo();
        }
        m_uri = formatPathInfo( pathInfo );
        
        for ( final Map.Entry< String, String > e : m_httpRequest.getQueryParams().entrySet() )
        {
            m_beanPropertyParameters.put(
                    NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( e.getKey() ), 
                    e.getValue() );
            try
            {
                final RequestParameterType rpt =
                        RequestParameterType.valueOf( NamingConventionType.CONSTANT.convert( e.getKey() ) );
                m_requestParameters.put(
                        rpt,
                        new RequestParameterValueImpl( this, rpt.getValueType(), e.getValue() ) );
            }
            catch ( final RuntimeException ex )
            {
                Validations.verifyNotNull( "Shut up CodePro", ex );
            }
        }

        m_restRequest = RestRequestImpl.valueOf( m_httpRequest.getType(), getRequestPath() );
        if ( getRestRequest().isValidRestRequest() )
        {
            m_bucketName = null;
            m_objectName = null;
        }
        else
        {
            final S3RequestPathParser requestPathParser = new S3RequestPathParser(
                    getRequestPath(),
                    m_httpRequest.getHeader( "bucket-name" ), 
                    m_httpRequest.getHeader( "object-name" ) );
            m_bucketName = requestPathParser.getBucketName();
            m_objectName = requestPathParser.getObjectName();
        }
    }
    
    
    static long getNextRequestId() 
    {
        synchronized ( REQUEST_ID_LOCK )
        {
            if ( Integer.MAX_VALUE == s_requestId )
            {
                LOG.warn( "Request IDs have wrapped from " + s_requestId + " to 0." );
                s_requestId = 0;
            }
            s_requestId += 1;
            return s_requestId;
        }
    }
    
    
    private static String getPathInfoFromOriginalUrl( final String url )
            throws MalformedURLException, DecoderException
    {
        if ( null == url )
        {
            return null;
        }
        else
        {
            //NOTE: percent encode any pluses since this url decoder turns them into spaces
            final String plusEscapedUrl = new URL( url ).getPath().replace( "+", "%2B" );
            return new String( URLCodec.decodeUrl( plusEscapedUrl.getBytes() ) );
        }
        
    }
    
    
    private static String formatPathInfo( final String pathInfo )
    {
        if ( null == pathInfo )
        {
            return null;
        }
        
        if ( pathInfo.startsWith( "/" ) )
        {
            return pathInfo;
        }
        return "/" + pathInfo;
    }

    
    public String getRequestPath()
    {
        return ( m_uri == null )? "" : m_uri;
    }

    
    public HttpRequest getHttpRequest()
    {
        return m_httpRequest;
    }

    
    public HttpServletResponse getHttpResponse()
    {
        return m_httpResponse;
    }

    
    public S3Authorization getAuthorization()
    {
        return m_authorization;
    }

    
    public long getRequestId()
    {
        return m_requestId;
    }
    

    public boolean hasRequestParameter( final RequestParameterType param )
    {
        Validations.verifyNotNull( "Param", param );
        return m_requestParameters.containsKey( param );
    }
    
    
    public Set< RequestParameterType > getRequestParameters()
    {
        return new HashSet<>( m_requestParameters.keySet() );
    }
    
    
    public RequestParameterValue getRequestParameter( final RequestParameterType param )
    {
        Validations.verifyNotNull( "Param", param );
        return m_requestParameters.get( param );
    }
    
    
    public String getRequestHeader( final S3HeaderType headerType )
    {
        final String headerName = headerType.getHttpHeaderName();
        return m_httpRequest.getHeader( headerName );
    }
    
    
    public Map< String, String > getBeanPropertyValueMapFromRequestParameters()
    {
        return new HashMap<>( m_beanPropertyParameters );
    }
    

    public String getRequestReceivedMessage( String handlerName )
    {
        if ( null == handlerName )
        {
            handlerName = "???";
        }
        m_handlerName = handlerName;
        
        final char lc = ' ';
        final String banner = LogUtil.getAlternateLogMessageHeaderBlock( '*',
                "HTTP Request #" + m_requestId + " Received   |   Handler: " + handlerName, null );
                
        final StringBuilder retval = new StringBuilder();
        retval.append( banner );
        retval.append( Platform.NEWLINE ).append( lc + " " ).append( m_httpRequest.getType() ).append( " " )
                                         .append( m_httpRequest.getOriginalClientRequestUrl() );
        retval.append( Platform.NEWLINE ).append( lc + " " ).append( "Headers:" );
        final List< String > sortedHeaderNames =  new ArrayList<>( m_httpRequest.getHeaders().keySet() );
        Collections.sort( sortedHeaderNames );
        for ( final String header : sortedHeaderNames )
        {
            retval.append( Platform.NEWLINE )
                  .append( lc + " " )
                  .append( "  " + header + ": '" + m_httpRequest.getHeader( header ) + "'" );
        }
        
        final List< String > sortedParameterNames = 
                new ArrayList<>( m_httpRequest.getQueryParams().keySet() );
        Collections.sort( sortedParameterNames );
        if ( !sortedParameterNames.isEmpty() )
        {
            retval.append( Platform.NEWLINE )
                  .append( lc + " " ).append( "Query Parameters:" );
            for ( final String parameter : sortedParameterNames )
            {
                final String value;                
                if (LogUtil.SECRETS.contains( parameter ) )
                {
                    value = LogUtil.CONCEALED;
                }
                else
                {
                    value = m_httpRequest.getQueryParam( parameter );
                }
                
                retval.append( Platform.NEWLINE ).append( lc + " " )
                      .append( "  " + parameter + ": " + ( ( null == value ) ? null : "'" + value + "'" ) );
            }
        }
        
        retval.append( Platform.NEWLINE ).append( lc + " " ).append( "Request:" ).append( Platform.NEWLINE );
        retval.append( lc + " " ).append( "  URL: " +
                LogUtil.hideSecretsInUrl(  m_httpRequest.getFullOriginalClientRequestUrl() ) );
        retval.append( Platform.NEWLINE ).append( lc + " " ).append( "  From: " );
        retval.append( m_httpRequest.getRemoteHost() );
        if ( !m_httpRequest.getRemoteAddr().equals( m_httpRequest.getRemoteHost() ) )
        {
            retval.append( " (" + m_httpRequest.getRemoteAddr() + ")" );
        }
        
        retval.append( Platform.NEWLINE );
        
        return Platform.NEWLINE + retval.toString();
    }
    

    public String getRequestProcessedMessage( final ServletResponseStrategy responseStrategy )
    {
        return m_handlerName + " processed request #" + m_requestId + " in " + m_timeInExistence  
               + ".  '" + responseStrategy.getServletNameToProvideResponseWith() 
               + "' will provide the response.";
    }
    
    
    public String getBucketName()
    {
        return m_bucketName;
    }
    
    
    public void setBucketName( final String bucketName )
    {
        m_bucketName = bucketName;
    }
    
    
    public String getObjectName()
    {
        return m_objectName;
    }
    
    
    public void setObjectName( final String objectName )
    {
        m_objectName = objectName;
    }
    
    
    public RestRequest getRestRequest()
    {
        return m_restRequest;
    }
    

    private final Duration m_timeInExistence = new Duration();
    private final HttpRequest m_httpRequest;
    private final HttpServletResponse m_httpResponse;
    private final S3Authorization m_authorization;
    private final String m_uri;
    private final long m_requestId;
    private final Map< RequestParameterType, RequestParameterValue > m_requestParameters = new HashMap<>();
    private final Map< String, String > m_beanPropertyParameters = new HashMap<>();
    private final RestRequest m_restRequest;
    
    private volatile String m_handlerName;
    private volatile String m_bucketName;
    private volatile String m_objectName;
    
    private static int s_requestId = 0;
    private static final Object REQUEST_ID_LOCK = new Object();
    private static final Logger LOG = Logger.getLogger(DS3Request.class);
}
