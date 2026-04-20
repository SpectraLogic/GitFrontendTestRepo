/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ClientRpcFuture_Test 
{
    @Test
    public void testConstructorNullTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new ClientRpcFuture( null, RpcMethodNullReturn.OPTIONAL, 1, "description" );
            }
            } );
    }
    
    
    @Test
    public void testConstructorNullReturnPolicyNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                    {
                        new ClientRpcFuture( String.class, null, 1, "description" );
                    }
                } );
    }
    

    @Test
    public void testConstructorNullDescriptionNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                    {
                        new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 1, null );
                    }
                } );
    }
    

    @Test
    public void testInitialStateReportedIsCorrect()
    {
        final long id = 2;
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, id, "description" );
        assertTrue(1 > future.getDuration().getElapsedSeconds(), "Duration shoulda just started.");
        assertEquals("description", future.getRequestDescription(), "Shoulda reported description.");
        assertEquals(id,  future.getRequestId(), "Shoulda reported id.");
    }
    
    
    @Test
    public void testGetWithoutBlockingDoesSoWhenResponseSet()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                    {
                        future.getWithoutBlocking();
                    }
                } );
        
        future.completedWithResponse( "hello" );
        assertEquals("hello", future.getWithoutBlocking(), "Shoulda reported id.");
    }
    
    
    @Test
    public void testGetWithoutBlockingDoesSoWhenErrorSet()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                future.getWithoutBlocking();
            }
        } );
        
        future.completedWithFailure( new RpcProxyException( "", BeanFactory.newBean( Failure.class )
                .setCode( "OOPS" ).setHttpResponseCode( 1 ).setMessage( "hiya" ) ) );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                future.getWithoutBlocking();
            }
        } );
    }
    
    
    @Test
    public void testIsDoneFalseUntilResponseSet()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        assertFalse(
                future.isDone(),
                "Should notta reported completed yet."
                 );
        future.completedWithResponse( "response" );
        assertTrue(future.isDone(), "Shoulda reported completed.");
    }
    
    
    @Test
    public void testIsDoneFalseUntilErrorSet()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        assertFalse(
                future.isDone(),
                "Should notta reported completed yet."
                 );
        future.completedWithFailure( new RpcProxyException( "", BeanFactory.newBean( Failure.class )
                .setCode( "OOPS" ).setHttpResponseCode( 1 ).setMessage( "hiya" ) ) );
        assertTrue(future.isDone(), "Shoulda reported completed.");
    }
    
    
    @Test
    public void testIsSuccessNullUntilResponseSet()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        assertEquals(null, future.isSuccess(), "Should notta reported completed yet.");
        future.completedWithResponse( "response" );
        assertEquals(Boolean.TRUE, future.isSuccess(), "Shoulda reported completed.");
    }
    
    
    @Test
    public void testIsSucessNullUntilErrorSet()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        assertEquals(null, future.isSuccess(), "Should notta reported completed yet.");
        future.completedWithFailure( new RpcProxyException( "", BeanFactory.newBean( Failure.class )
                .setCode( "OOPS" ).setHttpResponseCode( 1 ).setMessage( "hiya" ) ) );
        assertEquals(Boolean.FALSE, future.isSuccess(), "Shoulda reported completed.");
    }
    
    
    @Test
    public void testGetBlocksUntilTimeout()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        assertFalse(
                future.isTimeoutReachedByAtLeastOneClient(),
                "No client has timed out yet."
                );
        final Thread tLong = new Thread( new Runnable()
        {
            public void run()
            {
                future.get( Timeout.DEFAULT );
            }
        }, getClass().getSimpleName() + "-tLong" );
        
        final Throwable [] exceptions = new Throwable[ 1 ];
        final Thread tShort = new Thread( new Runnable()
        {
            public void run()
            {
                try
                {
                    future.get( 10, TimeUnit.MILLISECONDS );
                }
                catch ( final RpcTimeoutException ex )
                {
                    Validations.verifyNotNull( "Shut up CodePro.", ex );
                    exceptions[ 0 ] = ex;
                }
            }
        }, getClass().getSimpleName() + "-tShort" );
        
        tLong.start();
        tShort.start();

        assertEquals(true, tLong.isAlive(), "Thread shoulda been waiting.");
        assertEquals(true, tShort.isAlive(), "Thread shoulda been waiting.");

        int i = 100;
        while ( --i > 0 && tShort.isAlive() )
        {
            TestUtil.sleep( 10 );
        }

        assertEquals(true, tLong.isAlive(), "Thread shoulda been waiting.");
        assertEquals(false, tShort.isAlive(), "Thread shoulda timed out.");
        assertNotNull(
                exceptions[ 0 ],
                "Timeout exception shoulda occurred."
                 );
        assertTrue(future.isTimeoutReachedByAtLeastOneClient(), "A client timed out.");
    }
    
    
    @Test
    public void testGetBlocksUntilResponseSet()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        future.completedWithResponse( "hiya" );
        assertEquals("hiya", future.get( Timeout.DEFAULT ), "Shoulda returned correct response.");
    }
    
    
    @Test
    public void testGetBlocksUntilErrorSet()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        future.completedWithFailure( new RpcProxyException( "", BeanFactory.newBean( Failure.class )
                .setCode( "OOPS" ).setHttpResponseCode( 1 ).setMessage( "hiya" ) ) );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                future.get( Timeout.DEFAULT );
            }
             } );
    }
    
    
    @Test
    public void testSetNullResponseWhenMethodReturnPolicyNullRequiredAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.REQUIRED, 2, "description" );
        future.completedWithResponse( null );
        future.getWithoutBlocking();
    }
    
    
    @Test
    public void testSetNullResponseWhenMethodReturnPolicyNullOptionalAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        future.completedWithResponse( null );
        future.getWithoutBlocking();
    }
    
    
    @Test
    public void testSetNullResponseWhenMethodReturnPolicyNullDisallowedNotAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.DISALLOWED, 2, "description" );
        future.completedWithResponse( null );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
            {
                future.getWithoutBlocking();
            }
            } );
    }
    
    
    @Test
    public void testSetNonNullResponseWhenMethodReturnPolicyNullRequiredNotAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.REQUIRED, 2, "description" );
        future.completedWithResponse( "abc" );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
                {
                    future.getWithoutBlocking();
                }
            } );
    }
    
    
    @Test
    public void testSetNonNullResponseWhenMethodReturnPolicyNullOptionalAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        future.completedWithResponse( "abc" );
        future.getWithoutBlocking();
    }
    
    
    @Test
    public void testSetNonNullResponseWhenMethodReturnPolicyNullDisallowedAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.DISALLOWED, 2, "description" );
        future.completedWithResponse( "abc" );
        future.getWithoutBlocking();
    }
    
    
    @Test
    public void testSetResponseAfterCompletedNotAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        future.completedWithResponse( "hiya" );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                    {
                        future.completedWithResponse( "hiya" );
                    }
            } );
    }
    
    
    @Test
    public void testSetErrorAfterCompletedNotAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        future.completedWithResponse( "hiya" );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                future.completedWithFailure( new RpcProxyException( "", BeanFactory.newBean( Failure.class )
                        .setCode( "OOPS" ).setHttpResponseCode( 1 ).setMessage( "hiya" ) ) );
            }
        } );
    }
    
    
    @Test
    public void testAddListenerNullListenerNotAllowed()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    future.addRequestCompletedListener( null );
                }
            } );
    }
    
    
    @Test
    public void testListenersGetCalledUponCompleted()
    {
        final ClientRpcFuture future = 
                new ClientRpcFuture( String.class, RpcMethodNullReturn.OPTIONAL, 2, "description" );
        final BasicTestsInvocationHandler btih1 = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final RpcCompletedListener< Object > listener1 =
                InterfaceProxyFactory.getProxy( RpcCompletedListener.class, btih1 );
        final BasicTestsInvocationHandler btih2 = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final RpcCompletedListener< Object > listener2 =
                InterfaceProxyFactory.getProxy( RpcCompletedListener.class, btih2 );
        final BasicTestsInvocationHandler btih3 = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final RpcCompletedListener< Object > listener3 =
                InterfaceProxyFactory.getProxy( RpcCompletedListener.class, btih3 );
        
        future.addRequestCompletedListener( listener1 );
        future.addRequestCompletedListener( listener2 );
        
        final Method m = ReflectUtil.getMethod( 
                RpcCompletedListener.class, 
                "remoteProcedureRequestCompleted" );
        final Map< Method, Integer > expectedInitialCallCounts = new HashMap<>();
        expectedInitialCallCounts.put( m, Integer.valueOf( 0 ) );
        final Map< Method, Integer > expectedCompletedCallCounts = new HashMap<>();
        expectedCompletedCallCounts.put( m, Integer.valueOf( 1 ) );
        
        btih1.verifyMethodInvocations( expectedInitialCallCounts );
        btih2.verifyMethodInvocations( expectedInitialCallCounts );
        btih3.verifyMethodInvocations( expectedInitialCallCounts );
        
        future.completedWithResponse( null );
        
        btih1.verifyMethodInvocations( expectedCompletedCallCounts );
        btih2.verifyMethodInvocations( expectedCompletedCallCounts );
        btih3.verifyMethodInvocations( expectedInitialCallCounts );
        
        future.addRequestCompletedListener( listener3 );
        
        btih1.verifyMethodInvocations( expectedCompletedCallCounts );
        btih2.verifyMethodInvocations( expectedCompletedCallCounts );
        btih3.verifyMethodInvocations( expectedCompletedCallCounts );
    }
}
