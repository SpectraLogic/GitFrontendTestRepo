/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;

import org.junit.jupiter.api.Test;

import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestUtil;

import static org.junit.jupiter.api.Assertions.*;

public final class ClientAttackMitigator_Test 
{
    @Test
    public void testAttackDetectedAfterTooManyClientAuthorizationFailures()
    {
        final ClientAttackMitigator mitigator = new ClientAttackMitigator();
        mitigator.authorizationFailed( "user1", getMockRequest( "a" ) );
        assertFalse(mitigator.isAttackFromClient( getMockRequest( "a" ) ), "Should notta identified client as performing a DOS attack yet.");
        for ( int i = 0; i < 50; ++i )
        {
            mitigator.authorizationFailed( "user" + i, getMockRequest( "a" ) );
        }
        assertTrue(mitigator.isAttackFromClient( getMockRequest( "a" ) ), "Shoulda identified client as performing a DOS attack.");
        assertFalse(mitigator.isAttackOnUser( "user1" ), "Should notta identified DOS attack on user yet.");
        assertTrue(mitigator.isAttackFromClient( getMockRequest( "a" ) ), "Shoulda identified client as performing a DOS attack.");
    }
    
    
    @Test
    public void testAttackDetectedAfterTooManyUserAuthorizationFailures()
    {
        final ClientAttackMitigator mitigator = new ClientAttackMitigator();
        mitigator.authorizationFailed( "user", getMockRequest( "client00" ) );
        assertFalse(mitigator.isAttackOnUser( "user" ), "Should notta identified DOS attack on user yet.");
        for ( int i = 0; i < 50; ++i )
        {
            mitigator.authorizationFailed( "user", getMockRequest( "client" + i ) );
        }
        assertTrue(mitigator.isAttackOnUser( "user" ), "Shoulda identified DOS attack on user.");
        assertFalse(mitigator.isAttackFromClient( getMockRequest( "client00" ) ), "Should notta identified client as performing a DOS attack yet.");
        assertTrue(mitigator.isAttackOnUser( "user" ), "Shoulda identified DOS attack on user.");
    }
    
    
    @Test
    public void testAttackFromClientIsEventuallyCleared()
    {
        final ClientAttackMitigator mitigator = new ClientAttackMitigator( 0, 500 );
        assertFalse(mitigator.isAttackFromClient( getMockRequest( "a" ) ), "Should notta identified client as performing a DOS attack yet.");
        mitigator.authorizationFailed( "user1", getMockRequest( "a" ) );
        assertTrue(mitigator.isAttackFromClient( getMockRequest( "a" ) ), "Shoulda identified client as performing a DOS attack.");

        TestUtil.assertEventually( 
                10, 
                new Runnable()
                {
                    public void run()
                    {
                        assertFalse(mitigator.isAttackFromClient( getMockRequest( "a" ) ), "Shoulda cleared out DOS attack.");
                    }
                } );
    }
    
    
    @Test
    public void testAttackOnUserIsEventuallyCleared()
    {
        final ClientAttackMitigator mitigator = new ClientAttackMitigator( 0, 500 );
        assertFalse(mitigator.isAttackOnUser( "user" ), "Should notta identified DOS attack on user yet.");
        mitigator.authorizationFailed( "user", getMockRequest( "client00" ) );
        assertTrue(mitigator.isAttackOnUser( "user" ), "Shoulda identified DOS attack on user.");

        TestUtil.assertEventually( 
                10, 
                new Runnable()
                {
                    public void run()
                    {
                        assertFalse(mitigator.isAttackOnUser( "user" ), "Shoulda cleared out DOS attack.");
                    }
                } );
    }
    
    
    private HttpRequest getMockRequest( final String client )
    {
        return InterfaceProxyFactory.getProxy( 
                HttpRequest.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( HttpRequest.class, "getRemoteAddr" ),
                        new ConstantResponseInvocationHandler( client ),
                        null ) );
    }
}
