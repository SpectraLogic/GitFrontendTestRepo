/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.springframework.ui.ModelMap;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.spectralogic.s3.common.testfrmwrk.MockHttpServletRequest;
import com.spectralogic.s3.server.dispatch.RequestDispatcherImpl;
import com.spectralogic.s3.server.dispatch.S3Controller;
import com.spectralogic.s3.server.handler.reqhandler.ExcludeRequestHandlerResponseDocumentation;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.system.GetRequestHandlersRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.system.RequestHandlerExampleResponse;
import com.spectralogic.s3.server.servlet.BaseServlet;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.BeanServlet.BeanServletResponseListener;
import com.spectralogic.util.http.HttpHeaderType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.SortedProperties;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.marshal.Marshalable;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;


public final class MockHttpRequestDriver implements Runnable
{
    public MockHttpRequestDriver( 
            final MockHttpRequestSupport support,
            final boolean requestIsLocal,
            final MockAuthorizationStrategy authorizationStrategy,
            final RequestType requestType,
            final String requestPath )
    {
        m_requestLocal = requestIsLocal;
        m_requestType = requestType;
        m_requestPath = requestPath;
        m_authorizationStrategy = authorizationStrategy;
        m_support = support;
    }
    
    
    public MockHttpRequestDriver addHeader( final HttpHeaderType name, final String value )
    {
        return addHeader( name.getHttpHeaderName(), value );
    }
    
    
    public MockHttpRequestDriver addHeader( final String name, final String value )
    {
        m_headers.put( name, value );
        return this;
    }
    
    
    public MockHttpRequestDriver addParameter( final String name, final String value )
    {
        m_queryParameters.put( name, value );
        return this;
    }
    
    
    public MockHttpRequestDriver setRequestPayload( final byte[] requestPayload )
    {
        m_requestPayload = (requestPayload == null) ? null : requestPayload.clone();
        return this;
    }
    
    
    public String getResponseToClientAsString()
    {
        verifyHasRun();
        return m_responseToClient.getString();
    }
    
    
    public void assertResponseToClientHasHeaders( final Map< ?, String > requiredHeaders )
    {
        verifyHasRun();
        if ( m_responseHeadersToClient == null )
        {
            final List< MethodInvokeData > addHeaderInvocations = m_httpResponseBtih.getMethodInvokeData( 
                    ReflectUtil.getMethod( HttpServletResponse.class, "addHeader" ) );
            final Map< String, String > responseHeaders = new HashMap<>();
            for ( final MethodInvokeData invokeData : addHeaderInvocations )
            {
                final List< Object > args = invokeData.getArgs();
                responseHeaders.put( (String)args.get(0), (String)args.get(1) );
            }
            m_responseHeadersToClient = responseHeaders;
        }
        
        for ( final Map.Entry< ?, String > requiredHeader : requiredHeaders.entrySet() )
        {
            final String headerName = ( String.class == requiredHeader.getKey().getClass() ) ?
                    (String)requiredHeader.getKey()
                    : ( (HttpHeaderType)requiredHeader.getKey() ).getHttpHeaderName();
            final String responseHeaderValue = m_responseHeadersToClient.get( headerName );
            if ( responseHeaderValue == null )
            {
                throw new RuntimeException(
                        "Expected header with key '" + headerName + "' but it was missing." );
            }
            else if( !m_responseHeadersToClient.containsKey( headerName ) )
            {
                throw new RuntimeException(
                        "Expected unique header with key '" + headerName + "' but it was set twice." );
            }
            else if ( !requiredHeader.getValue().equals( responseHeaderValue ) )
            {
                throw new RuntimeException(
                        "Expected header with key " + headerName
                        + " to have value '" + requiredHeader.getValue()
                        + "' but it had value '" + responseHeaderValue + "' instead." );
            }
        }
    }
    
    
    public void assertResponseToClientMatches( final String regularExpression )
    {
        if ( !Pattern.matches( regularExpression, getResponseToClientAsString() ) )
        {
            throw new RuntimeException(
                    getHttpResponseCode()
                    + " <" + getResponseToClientAsString() + "> does not match <" + regularExpression 
                    + ">, but it should have." );
        }
    }
    
    
    public void assertResponseToClientDoesNotMatch( final String regularExpression )
    {
        if ( Pattern.matches( regularExpression, getResponseToClientAsString() ) )
        {
            throw new RuntimeException(
                    getHttpResponseCode()
                    + " <" + getResponseToClientAsString() + "> matches <" + regularExpression 
                    + ">, but it shouldn't have." );
        }
    }
    
    
    public void assertResponseToClientContains( final String substring )
    {
        if ( !getResponseToClientAsString().contains( substring ) )
        {
            throw new RuntimeException(
                    getHttpResponseCode()
                    + " <" + getResponseToClientAsString() + "> did not contain <" + substring 
                    + ">, but it should have." );
        }
    }
    
    
    public void assertResponseToClientXPathEquals( final String xPathExpression, final String expectedValue )
    {
        verifyHasRun();
        
        if ( m_xmlResponseDocument == null)
        {
            try
            {
                m_xmlResponseDocument = DocumentBuilderFactory
                        .newInstance()
                        .newDocumentBuilder()
                        .parse( IOUtils.toInputStream( getResponseToClientAsString(), "UTF-8" ) );
            }
            catch ( final SAXException | IOException | ParserConfigurationException ex )
            {
                throw new RuntimeException( ex );
            }
        }

        try
        {
            final String xPathResult = XPathFactory
                    .newInstance()
                    .newXPath()
                    .compile(xPathExpression)
                    .evaluate( m_xmlResponseDocument );
            if ( !xPathResult.equals( expectedValue ) )
            {
                throw new RuntimeException(
                        "Expected xpath expression '"
                        + xPathExpression + "' result to equal '"
                        + expectedValue + "', but it was '"
                        + xPathResult + "'." );
            }
        }
        catch ( final XPathExpressionException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    public void assertResponseToClientDoesNotContain( final String substring )
    {
        if ( getResponseToClientAsString().contains( substring ) )
        {
            throw new RuntimeException(
                    getHttpResponseCode()
                    + " <" + getResponseToClientAsString() + "> contains <" + substring 
                    + ">, but it shouldn't have." );
        }
    }
    
    
    public int getHttpResponseCode()
    {
        verifyHasRun();
        final List< MethodInvokeData > datas;
        try
        {
            datas = m_httpResponseBtih.getMethodInvokeData( 
                    HttpServletResponse.class.getMethod( "setStatus", int.class ) );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        return ( (Integer)datas.get( datas.size() - 1 ).getArgs().get( 0 ) ).intValue();
    }
    
    
    public void assertHttpResponseCodeEquals( final int expected )
    {
        assertHttpResponseCodeEquals( "", expected );
    }
    
    
    public void assertHttpResponseCodeEquals( final String prePend, final int expected )
    {
        verifyHasRun();
        if ( expected != getHttpResponseCode() )
        {
            throw new RuntimeException(
                    ( prePend == null) ? "" : prePend
                    + "Expected an HTTP response code of " + expected 
                    + ", but was " + getHttpResponseCode() + ". Client responded with message: " + getResponseToClientAsString() );
        }
    }
    
    
    public void run()
    {
        final ResponseTypeTracker responseTypeTracker = new ResponseTypeTracker();
        BeanServlet.setResponseTypeListener( responseTypeTracker );
        try
        {
            runInternal();
            m_ran = true;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }

        if ( null == EXAMPLE_RESPONSES )
        {
            verifyRunRecorded();
        }
        else
        {
            recordRun( responseTypeTracker );
        }
    }
    

    private final static class ResponseTypeTracker implements BeanServletResponseListener
    {
        public void responseReturned( final Object response )
        {
            if ( null == response )
            {
                m_type = null;
                return;
            }
            
            final Class< ? > type = getCorrectType( response.getClass() );
            if ( !type.isArray() || 0 == Array.getLength( response ) )
            {
                m_type = type;
            }
            else
            {
                m_type = Array.newInstance(
                        getCorrectType( Array.get( response, 0 ).getClass() ), 0 ).getClass();
            }
        }
        
        
        private Class< ? > getCorrectType( final Class< ? > clazz )
        {
            final Class< ? > retval = InterfaceProxyFactory.getType( clazz );
            if ( Marshalable.class.isAssignableFrom( retval ) && !retval.isInterface() )
            {
                if ( 0 == retval.getInterfaces().length )
                {
                    return retval;
                }
                if ( 1 == retval.getInterfaces().length )
                {
                    return retval.getInterfaces()[ 0 ];
                }
                throw new RuntimeException( "Failed to determine type to report for: " + retval );
            }
            
            return retval;
        }
        
        
        private String getHttpResponseTypeToReport()
        {
            final Class< ? > raw = m_type;
            if ( null == raw )
            {
                return "null";
            }
            return raw.getName();
        }
        
        
        private volatile Class< ? > m_type;
    } // end inner class def
    
    
    private void verifyRunRecorded()
    {
        final Class< ? > requestHandlerType = RequestDispatcherImpl.getLastRequestHandlerDispatchedTo();
        if ( null == requestHandlerType || null != requestHandlerType.getAnnotation(
                ExcludeRequestHandlerResponseDocumentation.class ) )
        {
            return;
        }
        synchronized ( EXAMPLE_RESPONSE_COUNTS )
        {
            if ( EXISTING_EXAMPLE_RESPONSE_KEYS.isEmpty() )
            {
                loadExistingExampleResponseKeys();
            }
            
            final String keyPrefix = getExampleResponseKeyPrefix( requestHandlerType );
            if ( !EXISTING_EXAMPLE_RESPONSE_KEYS.contains(
                    keyPrefix + RequestHandlerExampleResponse.HTTP_REQUEST ) )
            {
                throw new IllegalStateException(
                        "Example response documentation must be re-generated.  " 
                        + "See the javadocs for EXAMPLE_RESPONSES in " + getClass().getName() 
                        + " for instructions on how to re-generate response documentation manually, or "
                        + "use generateServerDocs.sh to re-generate response documentation automatically." );
            }
        }
    }
    
    
    private void loadExistingExampleResponseKeys()
    {
        final InputStream srPropertiesIs = 
                GetRequestHandlersRequestHandler.class.getResourceAsStream(
                        "/requesthandlerresponses.props" );
        final Properties srProperties = new Properties();
        try
        {
            srProperties.load( srPropertiesIs );
            srPropertiesIs.close();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to load request handler documentation.", ex );
        }
        
        for ( final Object key : srProperties.keySet() )
        {
            EXISTING_EXAMPLE_RESPONSE_KEYS.add( key.toString() );
        }
    }
    
    
    private void recordRun( final ResponseTypeTracker responseTypeTracker )
    {
        final Class< ? > requestHandlerType = RequestDispatcherImpl.getLastRequestHandlerDispatchedTo();
        if ( null == requestHandlerType || null != requestHandlerType.getAnnotation( 
                ExcludeRequestHandlerResponseDocumentation.class ) )
        {
            return;
        }
        assertResponseToClientHasHeaders( new HashMap< String, String >() );
        synchronized ( EXAMPLE_RESPONSE_COUNTS )
        {
            final String keyPrefix = getExampleResponseKeyPrefix( requestHandlerType );
            EXAMPLE_RESPONSES.put(
                    keyPrefix + RequestHandlerExampleResponse.TEST, 
                    getDescriptionForRecording() );
            EXAMPLE_RESPONSES.put(
                    keyPrefix + RequestHandlerExampleResponse.HTTP_REQUEST, 
                    m_requestType + " '" + m_requestPath + "' with query parameters " + m_queryParameters 
                    + " and headers " + m_headers + getRequestPayloadAsString() + "." );
            EXAMPLE_RESPONSES.put(
                    keyPrefix + RequestHandlerExampleResponse.HTTP_RESPONSE_CODE, 
                    String.valueOf( getHttpResponseCode() ) );
            EXAMPLE_RESPONSES.put(
                    keyPrefix + RequestHandlerExampleResponse.HTTP_RESPONSE, 
                    getResponseToClientAsString() + " with headers " + m_responseHeadersToClient + "." );
            EXAMPLE_RESPONSES.put(
                    keyPrefix + RequestHandlerExampleResponse.HTTP_RESPONSE_TYPE, 
                    responseTypeTracker.getHttpResponseTypeToReport() );
            
            final File outFile = new File(
                    System.getProperty( "java.io.tmpdir" ) + Platform.FILE_SEPARATOR 
                    + "requesthandlerresponses.props" );
            if ( outFile.exists() )
            {
                outFile.delete();
            }
            FileOutputStream out = null;
            try
            {
                out = new FileOutputStream( outFile );
                EXAMPLE_RESPONSES.store( out, getExampleResponsesFileComment() );
                final PrintStream systemOut = System.out;
                systemOut.println( "Generated request handler responses available at: " + outFile );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            finally
            {
                try
                {
                    if ( null != out )
                    {
                        out.close();
                    }
                }
                catch ( final IOException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
    }
    
    
    private String getRequestPayloadAsString()
    {
        if ( null == m_requestPayload )
        {
            return "";
        }
        
        try
        {
            return " and request payload {" 
                   + new String( m_requestPayload, Charset.forName( "UTF-8" ) ) + "}";
        }
        catch ( final Exception ex )
        {
            Validations.verifyNotNull( "Shut up CodePro.", ex );
            return " and request payload " + m_requestPayload.length + " bytes long";
        }
    }
    
    
    private String getDescriptionForRecording()
    {
        String retval = "unknown";
        for ( final StackTraceElement e : Thread.currentThread().getStackTrace() )
        {
            if ( e.getClassName().endsWith( "_Test" ) )
            {
                retval = e.getClassName() + "." + e.getMethodName();
            }
        }
        
        return retval;
    }
    
    
    private String getExampleResponseKeyPrefix( final Class< ? > requestHandlerType )
    {
        Integer responseCount = EXAMPLE_RESPONSE_COUNTS.get( requestHandlerType );
        if ( null == responseCount )
        {
            responseCount = Integer.valueOf( 0 );
        }
        EXAMPLE_RESPONSE_COUNTS.put( 
                requestHandlerType, 
                Integer.valueOf( responseCount.intValue() + 1 ) );
        return requestHandlerType.getName() + "." + responseCount + ".";
    }
    
    
    private String getExampleResponsesFileComment()
    {
        return "Auto-generated by " + getClass().getName() + " on " + new Date() + "." 
                + Platform.NEWLINE + Platform.NEWLINE 
                + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + Platform.NEWLINE
                + "!                                            !" + Platform.NEWLINE
                + "!     DO NOT MODIFY THIS FILE BY HAND!!!     !" + Platform.NEWLINE
                + "!                                            !" + Platform.NEWLINE
                + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + Platform.NEWLINE;
    }
    
    
    private void runInternal() throws Exception
    {
        m_authorizationStrategy.initializeDriver( this );
        
        final S3Controller controller = new S3Controller( m_support.getRequestDispatcher() );
        final ModelMap modelMap = new ModelMap();
        
        HttpServletRequest httpRequest =
                new MockHttpServletRequest( m_requestType, m_requestPath ).setHeaders( m_headers )
                .setLocalRequest( m_requestLocal ).setQueryParameters( m_queryParameters )
                .setRequestPayload( m_requestPayload ).generate();
        final HttpServletResponse httpResponse =
                InterfaceProxyFactory.getProxy( HttpServletResponse.class, m_httpResponseBtih );
        final String servletName = controller.handleS3Request( 
                httpRequest, 
                InterfaceProxyFactory.getProxy( HttpServletResponse.class, m_httpResponseBtih ),
                modelMap );
        
        final Object servletParams = modelMap.get( BaseServlet.class.getName() );
        httpRequest =
                new MockHttpServletRequest( m_requestType, m_requestPath ).setHeaders( m_headers )
                .setLocalRequest( m_requestLocal ).setQueryParameters( m_queryParameters )
                .setRequestPayload( m_requestPayload )
                .putAttributeResponse( BaseServlet.class.getName(), servletParams ).generate();
        
        final Class< ? > servletClass = Class.forName(
                BaseServlet.class.getPackage().getName() + "." 
                + NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( servletName ) );
        final BaseServlet< ? > servlet = (BaseServlet< ? >)servletClass.newInstance();
        switch ( m_requestType )
        {
            case GET:
                servlet.doGet( httpRequest, httpResponse );
                break;
            case DELETE:
                servlet.doDelete( httpRequest, httpResponse );
                break;
            case HEAD:
                servlet.doHead( httpRequest, httpResponse );
                break;
            case POST:
                servlet.doPost( httpRequest, httpResponse );
                break;
            case PUT:
                servlet.doPut( httpRequest, httpResponse );
                break;
            default:
                throw new UnsupportedOperationException( "No code for " + m_requestType + "." );
        }
    }
    
    
    private void verifyHasRun()
    {
        if ( !m_ran )
        {
            throw new IllegalStateException( "You must run the " + getClass().getSimpleName() + " first." );
        }
    }
    
    
    private volatile boolean m_ran;
    
    private final boolean m_requestLocal;
    private final String m_requestPath;
    private final RequestType m_requestType;
    private final MockHttpServletOutputStream m_responseToClient = new MockHttpServletOutputStream();
    private volatile Document m_xmlResponseDocument = null;
    private volatile Map< String, String > m_responseHeadersToClient = null;
    private final BasicTestsInvocationHandler m_httpResponseBtih =
            new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                    ReflectUtil.getMethod( HttpServletResponse.class, "getOutputStream" ),
                    new ConstantResponseInvocationHandler( m_responseToClient ),
                    null ) );
    private final MockAuthorizationStrategy m_authorizationStrategy;
    private final Map< String, String > m_headers = new HashMap<>();
    private final Map< String, String > m_queryParameters = new HashMap<>();
    private byte[] m_requestPayload = null;
    private final MockHttpRequestSupport m_support;
    
    /**
     * Set this to null if you don't want to generate example responses.  Set this to a new 
     * {@link SortedProperties} if you do want to generate example responses.  In Perforce, this should always
     * be set to null.  <br><br>
     * 
     * Instructions to re-generate example responses:
     * <ol>
     * <li> Uncomment out the <code>new SortedProperties()</code> below
     * <li> Run all tests under the S3 server, either in Eclipse or from the command line
     * <li> The console output will tell you the new location of the example responses
     * <li> Copy the entire contents of the new example responses file from the step above into the
     *      SCM-managed one
     * <li> Revert the change you made to this file
     * </ol>
     */
    private final static Properties EXAMPLE_RESPONSES = null; //new SortedProperties();
    private final static Set< String > EXISTING_EXAMPLE_RESPONSE_KEYS = new HashSet<>();
    private final static Map< Class< ? >, Integer > EXAMPLE_RESPONSE_COUNTS = new HashMap<>();
}
