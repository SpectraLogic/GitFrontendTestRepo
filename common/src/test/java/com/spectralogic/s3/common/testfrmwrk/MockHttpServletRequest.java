package com.spectralogic.s3.common.testfrmwrk;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.IteratorEnumeration;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class MockHttpServletRequest
{
    public MockHttpServletRequest( final RequestType requestType, final String url )
    {
        m_requestType = requestType;
        m_url = url;
    }
    
    
    public MockHttpServletRequest setLocalRequest( final boolean value )
    {
        m_localRequest = value;
        return this;
    }
    
    
    public MockHttpServletRequest setHeaders( final Map< String, String > value )
    {
        m_headers = value;
        return this;
    }
    
    
    public MockHttpServletRequest setQueryParameters( final Map< String, String > queryParameters )
    {
        m_queryParameters = queryParameters;
        return this;
    }
    
    
    public MockHttpServletRequest setRequestPayload( final byte[] requestPayload )
    {
        m_requestPayload = (requestPayload == null) ? null : requestPayload.clone();
        return this;
    }
    
    
    public MockHttpServletRequest setClient( final String client )
    {
        m_client = client;
        return this;
    }
    
    
    public MockHttpServletRequest putAttributeResponse( final String key, final Object response )
    {
        m_attributeResponses.put( key, response );
        return this;
    }
    
    
    public HttpServletRequest generate()
    {
        final String remoteAddress = ( m_localRequest ) ? "127.0.0.1" : "192.168.1.22";
        InvocationHandler ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod(
                        HttpServletRequest.class, "getParameterMap" ),
                        new ConstantResponseInvocationHandler( new HashMap<>() ), 
                        null );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getPathInfo" ),
                new ConstantResponseInvocationHandler( m_url ),
                ih );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getRequestURI" ),
                new ConstantResponseInvocationHandler( m_url ), 
                ih );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getRequestURL" ),
                new ConstantResponseInvocationHandler( new StringBuffer( ( null == m_url ) ? "" : m_url ) ), 
                ih );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getAttribute" ),
                new InvocationHandler()
                {
                    @Override
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        return m_attributeResponses.get( args[ 0 ] );
                    }
                },
                ih );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getInputStream" ),
                new ConstantResponseInvocationHandler(
                        new OptionalByteArrayServletInputStream( m_requestPayload ) ),
                ih );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getMethod" ),
                new ConstantResponseInvocationHandler( m_requestType.toString() ),
                ih );
        ih = MockInvocationHandler.forMethod( 
                ReflectUtil.getMethod( HttpServletRequest.class, "getHeaderNames" ),
                new InvocationHandler()
                {
                    public Object invoke(Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        if ( null == m_headers )
                        {
                            return new IteratorEnumeration<>( new HashSet<>().iterator() );
                        }
                        return new IteratorEnumeration<>( m_headers.keySet().iterator() );
                    }
                },
                ih );
        ih = MockInvocationHandler.forMethod( 
                ReflectUtil.getMethod( HttpServletRequest.class, "getContentType" ),
                new InvocationHandler()
                {
                    public Object invoke(Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        if ( null == m_headers )
                        {
                            return null;
                        }
                        return m_headers.get(
                                S3HeaderType.CONTENT_TYPE.getHttpHeaderName() );
                    }
                },
                ih );
        ih = MockInvocationHandler.forMethod( 
                ReflectUtil.getMethod( HttpServletRequest.class, "getRemoteAddr" ), 
                new ConstantResponseInvocationHandler( ( null == m_client ) ? remoteAddress : m_client ),
                ih );
        ih = MockInvocationHandler.forMethod( 
                ReflectUtil.getMethod( HttpServletRequest.class, "getParameter" ), 
                new InvocationHandler()
                {
                    public Object invoke(Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        if ( null == m_queryParameters )
                        {
                            return null;
                        }
                        return m_queryParameters.get( args[ 0 ] );
                    }
                },
                ih );
        ih = MockInvocationHandler.forMethod( 
                ReflectUtil.getMethod( HttpServletRequest.class, "getParameterMap" ), 
                new InvocationHandler()
                {
                    public Object invoke(Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        if ( null == m_queryParameters )
                        {
                            return new HashMap<>();
                        }
                        final Map< String, String [] > retval = new HashMap<>();
                        for ( final Map.Entry< String, String > e : m_queryParameters.entrySet() )
                        {
                            retval.put( e.getKey(), new String [] { e.getValue() } );
                        }
                        return retval;
                    }
                },
                ih );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getHeader" ),
                new InvocationHandler()
                {
                    public Object invoke(Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        if ( S3HeaderType.CONTENT_LENGTH.getHttpHeaderName().equals( 
                                args[ 0 ] ) )
                        {
                            if ( null == m_headers || !m_headers.containsKey(
                                    S3HeaderType.CONTENT_LENGTH.getHttpHeaderName() ) )
                            {
                                return "0";
                            }
                        }
                        
                        if ( null == m_headers )
                        {
                            return null;
                        }
                        return m_headers.get( args[ 0 ] );
                    }
                },
                ih );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getHeaders" ),
                new InvocationHandler()
                {
                    public Object invoke(Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        if ( S3HeaderType.CONTENT_LENGTH.getHttpHeaderName().equals(
                                args[ 0 ] ) )
                        {
                            if ( null == m_headers || !m_headers.containsKey(
                                    S3HeaderType.CONTENT_LENGTH.getHttpHeaderName() ) )
                            {
                                return "0";
                            }
                        }

                        if ( null == m_headers )
                        {
                            return null;
                        }

                        return new IteratorEnumeration<>( CollectionFactory.toSet( m_headers.get( args[ 0 ] ) ).iterator() );
                    }
                },
                ih );
        return InterfaceProxyFactory.getProxy( 
                HttpServletRequest.class,
                ih );
    }
    
    
    private final class OptionalByteArrayServletInputStream extends ServletInputStream
    {
        private OptionalByteArrayServletInputStream(final byte[] innerInputStream)
        {
            m_innerInputStream = (innerInputStream == null) ?
                    null
                    : new ByteArrayInputStream( innerInputStream );
        }
        
        
        @Override
        public int read() throws IOException
        {
            return (m_innerInputStream == null) ? -1 : m_innerInputStream.read();
        }
        
        
        private final ByteArrayInputStream m_innerInputStream;
    }// end inner class
    

    private volatile String m_client;
    private volatile boolean m_localRequest;
    private volatile Map< String, String > m_headers;
    private final RequestType m_requestType;
    private final String m_url;
    private volatile Map< String, String > m_queryParameters;
    private volatile byte[] m_requestPayload;
    private final Map< String, Object > m_attributeResponses = new HashMap<>();
}