/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.authorization;

import java.util.HashMap;
import java.util.Map;



import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockHttpServletRequest;
import com.spectralogic.s3.server.authorization.api.AuthorizationValidationStrategy;
import com.spectralogic.s3.server.authorization.api.S3Authorization;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.http.ServletHttpRequest;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class S3AuthorizationImpl_Test
{
    @Test
    public void testNoAuthorizationHeaderResultsInNullAuthIdAndSignature()
    {
        final S3Authorization authorization = new S3AuthorizationImpl( 
                new ServletHttpRequest(
                        new MockHttpServletRequest( RequestType.GET, null ).generate() ) );
        
        final String [] idAndAuthorization = getParsedAuthorizationIdAndSignature( authorization );
        assertEquals(null, idAndAuthorization[ 0 ], "Shoulda parsed id and authorization from header.");
        assertEquals(null, idAndAuthorization[ 1 ], "Shoulda parsed id and authorization from header.");
    }
    
    
    @Test
    public void testEmptyAuthorizationHeaderResultsInNullAuthIdAndSignature()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.AUTHORIZATION.getHttpHeaderName(), "" );
        final S3Authorization authorization = new S3AuthorizationImpl( 
                new ServletHttpRequest( 
                        new MockHttpServletRequest( RequestType.GET, null )
                        .setHeaders( headers ).generate() ) );
        
        final String [] idAndAuthorization = getParsedAuthorizationIdAndSignature( authorization );
        assertEquals(null, idAndAuthorization[ 0 ], "Shoulda parsed id and authorization from header.");
        assertEquals(null, idAndAuthorization[ 1 ], "Shoulda parsed id and authorization from header.");
    }
    

    @Test
    public void testWhitespaceAuthorizationHeaderResultsInNullAuthIdAndSignature()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.AUTHORIZATION.getHttpHeaderName(), "   " );
        final S3Authorization authorization = new S3AuthorizationImpl( 
                new ServletHttpRequest( 
                        new MockHttpServletRequest( RequestType.GET, null )
                        .setHeaders( headers ).generate() ) );
        
        final String [] idAndAuthorization = getParsedAuthorizationIdAndSignature( authorization );
        assertEquals(null, idAndAuthorization[ 0 ], "Shoulda parsed id and authorization from header.");
        assertEquals(null, idAndAuthorization[ 1 ], "Shoulda parsed id and authorization from header.");
    }
    
    
    @Test
    public void testMalformedAuthorizationHeaderResultsInIdWithoutSignature()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.AUTHORIZATION.getHttpHeaderName(), "foobar" );
        final S3Authorization authorization = new S3AuthorizationImpl( 
                new ServletHttpRequest( 
                        new MockHttpServletRequest( RequestType.GET, null )
                        .setHeaders( headers ).generate() ) );
        
        final String [] idAndAuthorization = getParsedAuthorizationIdAndSignature( authorization );
        assertEquals("foobar", idAndAuthorization[ 0 ], "Shoulda parsed id and authorization from header.");
        assertEquals(null, idAndAuthorization[ 1 ], "Shoulda parsed id and authorization from header.");
    }
    
    
    @Test
    public void testAuthorizationHeaderProcessedCorrectlyWhenAwsPrefix()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( 
                S3HeaderType.AUTHORIZATION.getHttpHeaderName(), 
                "AWS authId:authsignature" );
        final S3Authorization authorization = new S3AuthorizationImpl( 
                new ServletHttpRequest( 
                        new MockHttpServletRequest( RequestType.GET, null )
                        .setHeaders( headers ).generate() ) );
        
        final String [] idAndAuthorization = getParsedAuthorizationIdAndSignature( authorization );
        assertEquals("authId", idAndAuthorization[ 0 ], "Shoulda parsed id and authorization from header.");
        assertEquals("authsignature", idAndAuthorization[ 1 ], "Shoulda parsed id and authorization from header.");
    }
    
    
    @Test
    public void testAuthorizationHeaderProcessedCorrectlyWhenNoAwsPrefix()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( 
                S3HeaderType.AUTHORIZATION.getHttpHeaderName(), 
                "authId:authsignature" );
        final S3Authorization authorization = new S3AuthorizationImpl( 
                new ServletHttpRequest( 
                        new MockHttpServletRequest( RequestType.GET, null )
                        .setHeaders( headers ).generate() ) );
        
        final String [] idAndAuthorization = getParsedAuthorizationIdAndSignature( authorization );
        assertEquals("authId", idAndAuthorization[ 0 ], "Shoulda parsed id and authorization from header.");
        assertEquals("authsignature", idAndAuthorization[ 1 ], "Shoulda parsed id and authorization from header.");
    }
    
    
    private String [] getParsedAuthorizationIdAndSignature( final S3Authorization authorization )
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final AuthorizationValidationStrategy strategy = 
                InterfaceProxyFactory.getProxy( AuthorizationValidationStrategy.class, btih );
        authorization.validate( strategy );
        return new String [] 
                { 
                    (String)btih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ),
                    (String)btih.getMethodInvokeData().get( 0 ).getArgs().get( 2 ) 
                };
    }
}
