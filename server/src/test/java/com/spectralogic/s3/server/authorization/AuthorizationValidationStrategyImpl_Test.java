/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.authorization;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;


import org.apache.commons.codec.binary.Base64;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.common.testfrmwrk.MockHttpServletRequest;
import com.spectralogic.s3.server.authorization.api.AuthorizationValidationStrategy;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.http.ServletHttpRequest;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class AuthorizationValidationStrategyImpl_Test
{
    @Test
    public void testNullIdAndNullSignatureResultsInPass()
    {
        final AuthorizationValidationStrategy strategy = 
                new AuthorizationValidationStrategyImpl( getUserRetriever( null ) );
        assertEquals(null, strategy.getAuthorization(
                        getHttpRequest( new Date() ),
                        null,
                        null ), "Shoulda reported no authorization.");
    }
    
    
    @Test
    public void testNullIdAndNonNullSignatureResultsInPass()
    {
        final AuthorizationValidationStrategy strategy = 
                new AuthorizationValidationStrategyImpl( getUserRetriever( null ) );

        assertEquals(null, strategy.getAuthorization(
                        getHttpRequest( new Date() ),
                        null,
                        "signature" ), "Shoulda reported no authorization.");
    }
    
    
    @Test
    public void testNonNullIdAndNullSignatureResultsInFail()
    {
        final User user = BeanFactory.newBean( User.class );
        user.setAuthId( "id" );
        user.setSecretKey( "secret" );
        final AuthorizationValidationStrategy strategy = 
                new AuthorizationValidationStrategyImpl( getUserRetriever( user ) );
        TestUtil.assertThrows( null, S3RestException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                strategy.getAuthorization(
                        getHttpRequest( new Date() ), 
                        "id", 
                        null );
            }
        } );
    }
    
    
    @Test
    public void testNonNullIdAndInvalidNonNullSignatureResultsInFail()
    {
        final User user = BeanFactory.newBean( User.class );
        user.setAuthId( "id" );
        user.setSecretKey( "secret" );
        final AuthorizationValidationStrategy strategy = 
                new AuthorizationValidationStrategyImpl( getUserRetriever( user ) );
        TestUtil.assertThrows( null, S3RestException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                strategy.getAuthorization(
                        getHttpRequest( new Date() ), 
                        "id", 
                        "secret" );
            }
        } );
    }
    
    
    @Test
    public void testNonNullIdAndValidNonNullSignatureResultsInPass()
    {
        final User user = BeanFactory.newBean( User.class );
        user.setAuthId( "id" );
        user.setSecretKey( "secret" );
        final AuthorizationValidationStrategy strategy = 
                new AuthorizationValidationStrategyImpl( getUserRetriever( user ) );
        
        final Date date = new Date();
        assertEquals(user, strategy.getAuthorization(
                        getHttpRequest( date ),
                        "id",
                        sign( "GET\n\n\n" + date.toString() + "\n", "secret" ) ), "Shoulda returned non-null authorization.");
    }
    

    private String sign( final String stringToSign, final String secretKey )
    {
        try
        {
            final byte[] secretyKeyBytes = secretKey.getBytes( "UTF8" );
            final SecretKeySpec secretKeySpec = new SecretKeySpec( secretyKeyBytes, "HmacSHA1" );
            final Mac mac = Mac.getInstance( "HmacSHA1" );
            mac.init(secretKeySpec);
            
            final byte[] data = stringToSign.getBytes( "UTF8" );
            final byte[] rawHmac = mac.doFinal( data );
            return Base64.encodeBase64String( rawHmac );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private HttpRequest getHttpRequest( final Date date )
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( "DATE", date.toString() );
        return new ServletHttpRequest( new MockHttpServletRequest(
                RequestType.GET, "http://www.jstevens.net" ).setHeaders( headers )
                .generate() );
    }
    
    
    private UserService getUserRetriever( final User userToReturn )
    {
        return InterfaceProxyFactory.getProxy(
                UserService.class,
                MockInvocationHandler.forReturnType(
                        SimpleBeanSafeToProxy.class,
                        new ConstantResponseInvocationHandler( userToReturn ),
                        null ) );
    }
}
