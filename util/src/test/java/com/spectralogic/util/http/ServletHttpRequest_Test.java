/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.http;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.IteratorEnumeration;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ServletHttpRequest_Test 
{
    @Test
    public void testConstructorNullServletRequestNotAllowed()
    {
        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test()
                {
                    new ServletHttpRequest( null );
                }
            } );
    }
    
    
    @Test
    public void testGetContentTypeDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate = ReflectUtil.getMethod( HttpServletRequest.class, "getContentType" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate), "Should notta made any delegating call onto the backing request yet.");

        request.getContentType();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda made a single delegating call onto the backing request.");
    }



    @Test
    public void testGetInputStreamDelegatesToBackingRequest() throws IOException
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate = ReflectUtil.getMethod( HttpServletRequest.class, "getInputStream" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate), "Should notta made any delegating call onto the backing request yet.");

        request.getInputStream();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda made a single delegating call onto the backing request.");
    }
    
    
    @Test
    public void testGetOriginalClientRequestUrlDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate1 = ReflectUtil.getMethod( HttpServletRequest.class, "getRequestURL" );
        final Method methodDelegate2 = ReflectUtil.getMethod( HttpServletRequest.class, "getQueryString" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate1), "Should notta made any delegating call onto the backing request yet.");
        assertEquals(0,  btih.getMethodCallCount(methodDelegate2), "Should never have called method.");

        request.getOriginalClientRequestUrl();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate1), "Shoulda made a single delegating call onto the backing request.");
        assertEquals(0,  btih.getMethodCallCount(methodDelegate2), "Should never have called method.");
    }
    
    
    @Test
    public void testGetFullOriginalClientRequestUrlDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate1 = ReflectUtil.getMethod( HttpServletRequest.class, "getRequestURL" );
        final Method methodDelegate2 = ReflectUtil.getMethod( HttpServletRequest.class, "getQueryString" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate1), "Should notta made any delegating call onto the backing request yet.");
        assertEquals(0,  btih.getMethodCallCount(methodDelegate2), "Should notta made any delegating call onto the backing request yet.");

        request.getFullOriginalClientRequestUrl();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate1), "Shoulda made a single delegating call onto the backing request.");
        assertEquals(1,  btih.getMethodCallCount(methodDelegate2), "Shoulda made a single delegating call onto the backing request.");
    }
    
    
    @Test
    public void testGetPathInfoDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate = ReflectUtil.getMethod( HttpServletRequest.class, "getPathInfo" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate), "Should notta made any delegating call onto the backing request yet.");

        request.getPathInfo();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda made a single delegating call onto the backing request.");
    }
    
    
    @Test
    public void testGetRemoteAddrDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate = ReflectUtil.getMethod( HttpServletRequest.class, "getRemoteAddr" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate), "Should notta made any delegating call onto the backing request yet.");

        request.getRemoteAddr();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda made a single delegating call onto the backing request.");
    }
    
    
    @Test
    public void testGetRemoteHostDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate = ReflectUtil.getMethod( HttpServletRequest.class, "getRemoteHost" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate), "Should notta made any delegating call onto the backing request yet.");

        request.getRemoteHost();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda made a single delegating call onto the backing request.");
    }
    
    
    @Test
    public void testGetRemotePortDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate = ReflectUtil.getMethod( HttpServletRequest.class, "getRemotePort" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate), "Should notta made any delegating call onto the backing request yet.");

        request.getRemotePort();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda made a single delegating call onto the backing request.");
    }
    
    
    @Test
    public void testGetRemoteUserDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate = ReflectUtil.getMethod( HttpServletRequest.class, "getRemoteUser" );
        assertEquals(0,  btih.getMethodCallCount(methodDelegate), "Should notta made any delegating call onto the backing request yet.");

        request.getRemoteUser();

        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda made a single delegating call onto the backing request.");
    }
    
    
    @Test
    public void testGetTypeDelegatesToBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        final Method methodDelegate = ReflectUtil.getMethod( HttpServletRequest.class, "getMethod" );
        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda made a single delegating call onto the backing request.");

        assertEquals(RequestType.GET, request.getType(), "Shoulda returned value returned from delegate.");

        assertEquals(1,  btih.getMethodCallCount(methodDelegate), "Shoulda cached result of determining the type.");
    }
    
    
    @Test
    public void testHeadersCannotBeModifiedByClient()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                request.getHeaders().clear();
            }
        } );
    }
    
    
    @Test
    public void testParametersCannotBeModifiedByClient()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );
        
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                request.getQueryParams().clear();
            }
        } );
    }
    
    
    @Test
    public void testHeadersReturnedMatchWhatIsExpectedBasedOnBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );

        assertEquals(2,  request.getHeaders().size(), "Should identified explicit header and header specified via query param.");
        assertEquals("hv,hw", request.getHeader( "a" ), "Should identified explicit header and header specified via query param.");
        assertEquals("hv,hw", request.getHeader( "A" ), "Should identified explicit header and header specified via query param.");
        assertFalse(
                request.getHeaders().keySet().contains( "b" ),
                "Should identified explicit header and header specified via query param.");
        assertFalse(
                request.getHeaders().keySet().contains( "B" ),
                "Should identified explicit header and header specified via query param.");
        assertEquals("abc", request.getHeader( "b" ), "Should identified explicit header and header specified via query param.");
        assertEquals("abc", request.getHeader( "B" ), "Should identified explicit header and header specified via query param.");
        assertFalse(
                request.getHeaders().keySet().contains( "c" ),
                "Should identified explicit header and header specified via query param.");
        assertFalse(
                request.getHeaders().keySet().contains( "C" ),
                "Should identified explicit header and header specified via query param.");
        assertEquals("abc", request.getHeader( "c" ), "Should identified explicit header and header specified via query param.");
        assertEquals("abc", request.getHeader( "C" ), "Should identified explicit header and header specified via query param.");
        assertEquals("qhv", request.getHeader( "d" ), "Should identified explicit header and header specified via query param.");
        assertEquals("qhv", request.getHeader( "D" ), "Should identified explicit header and header specified via query param.");
    }
    
    
    @Test
    public void testQueryParamsReturnedMatchWhatIsExpectedBasedOnBackingRequest()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBackingRequestIh() );
        final HttpServletRequest backingRequest =
                InterfaceProxyFactory.getProxy( HttpServletRequest.class, btih );
        final ServletHttpRequest request = new ServletHttpRequest( backingRequest );

        assertEquals(2,  request.getQueryParams().size(), "Should identified the 2 params that weren't http header overrides.");
        assertEquals(null, request.getQueryParam( "a" ), "Should identified the 2 params that weren't http header overrides.");
        assertEquals("pv", request.getQueryParam( "b" ), "Should identified the 2 params that weren't http header overrides.");
        assertEquals("pv1", request.getQueryParam( "c" ), "Should identified the 2 params that weren't http header overrides.");
        assertEquals(null, request.getQueryParam( "d" ), "Should identified the 2 params that weren't http header overrides.");
    }


    private InvocationHandler getBackingRequestIh()
    {
        final Map< String, String [] > params = new HashMap<>();
        params.put( "b", new String [] { "pv" } );
        params.put( "c", new String [] { "pv1", "pv2" } );
        params.put( "http-header-D", new String [] { "qhv", "invalid" } );
        
        return MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getMethod" ),
                new ConstantResponseInvocationHandler( "GET" ),
                MockInvocationHandler.forMethod( 
                    ReflectUtil.getMethod( HttpServletRequest.class, "getHeaderNames" ),
                    new ConstantResponseInvocationHandler( 
                        new IteratorEnumeration<>( CollectionFactory.toSet( "A" ).iterator() ) ),
                    MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( HttpServletRequest.class, "getHeader" ),
                        new ConstantResponseInvocationHandler( "abc" ),
                        MockInvocationHandler.forMethod(
                            ReflectUtil.getMethod( HttpServletRequest.class, "getHeaders" ),
                            new ConstantResponseInvocationHandler(
                                new IteratorEnumeration<>( CollectionFactory.toSet( "hv", "hw" ).iterator() ) ),
                            MockInvocationHandler.forMethod(
                                ReflectUtil.getMethod( HttpServletRequest.class, "getParameterMap" ),
                                new ConstantResponseInvocationHandler( params ),
                                null ) ) ) ) );
    }
}
