/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironmentManager;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeMoveStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class BaseTapeMoveStrategy_Test 
{
    @Test
    public void testAddListenerNullListenerNotAllowed()
    {
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                strategy.addListener( null );
            }
        } );
    }
    
    
    @Test
    public void testMoveFailedAfterMoveSucceededNotAllowed()
    {
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        strategy.getDest(
                666, 
                tape,
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, null ) );
        strategy.moveSucceeded();
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                strategy.moveFailed( tape );
            }
        } );
    }
    
    @Test
    public void testMovedSucceededAfterMoveFailedNotAllowed()
    {
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        strategy.getDest(
                666, 
                tape,
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, null ) );
        strategy.moveFailed( tape );
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                strategy.moveSucceeded();
            }
        } );
    }
    
    @Test
    public void testValidationCompletedAfterMoveFailedNotAllowed()
    {
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        strategy.moveFailed( tape );
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                strategy.getDest(
                        666, 
                        tape,
                        InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, null ) );
            }
        } );
    }
    
    @Test
    public void testMoveFailedWhereTapeIsDifferentFromOriginalNotAllowed()
    {
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        strategy.getDest(
                666, 
                tape,
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, null ) );
        
        final Tape tape2 = BeanFactory.newBean( Tape.class );
        tape2.setId( UUID.randomUUID() );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                strategy.moveFailed( tape2 );
            }
        } );
    }
    
    @Test
    public void 
    testAddedListenerReceivesFailedMoveNotificationRegardlessAsToWhenListenerWasAddedOrOtherListeners()
            throws InterruptedException
    {
        final int listenerCount = 100;
        final CountDownLatch latch = new CountDownLatch( listenerCount );
        final List< BasicTestsInvocationHandler > nonExceptionThrowingBtihs = new CopyOnWriteArrayList<>();
        final List< BasicTestsInvocationHandler > listenerBtihs = new CopyOnWriteArrayList<>();
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        final Method methodValidationCompleted = 
                ReflectUtil.getMethod( TapeMoveListener.class, "validationCompleted" );
        final Method methodMoveSucceeded = 
                ReflectUtil.getMethod( TapeMoveListener.class, "moveSucceeded" );
        final Method methodMoveFailed = 
                ReflectUtil.getMethod( TapeMoveListener.class, "moveFailed" );
        for ( int i = 0; i < listenerCount; ++i )
        {
            final boolean throwsException = ( 0 == i % 2 );
            final ListenerAdder listenerAdder = new ListenerAdder( latch, strategy, throwsException );
            listenerBtihs.add( listenerAdder.m_btih );
            if ( !throwsException )
            {
                nonExceptionThrowingBtihs.add( listenerAdder.m_btih );
            }
            SystemWorkPool.getInstance().submit( listenerAdder );
        }
        
        TestUtil.sleep( 1 );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        strategy.moveFailed( tape );
        latch.await();
        
        for ( final BasicTestsInvocationHandler btih : listenerBtihs )
        {
            assertEquals(1,  btih.getMethodCallCount(methodValidationCompleted), "Listener shoulda been notified of validationCompleted event no matter what.");
            assertEquals(0,  btih.getMethodCallCount(methodMoveFailed), "Listener should notta been notified of failure event since no failure occurred.");
        }
        for ( final BasicTestsInvocationHandler btih : nonExceptionThrowingBtihs )
        {
            assertEquals(1,  btih.getMethodCallCount(methodValidationCompleted), "Listener shoulda been notified of validationCompleted event no matter what.");
            final Object expected = tape.getId();
            assertEquals(expected, btih.getMethodInvokeData( methodValidationCompleted ).get( 0 ).getArgs().get( 0 ), "Validation completed event shoulda been a failure.");
            assertNotNull(btih.getMethodInvokeData( methodValidationCompleted ).get( 0 ).getArgs().get( 1 ), "Validation completed event shoulda been a failure.");
            assertEquals(0,  btih.getMethodCallCount(methodMoveSucceeded), "Listener should notta been notified of success event since no success occurred.");
        }
    }
    
    @Test
    public void 
    testAddedListenerReceivesFailedValidationNotificationRegardlessAsToWhenListenerWasAddedOrOtherListeners()
            throws InterruptedException
    {
        final int listenerCount = 100;
        final CountDownLatch latch = new CountDownLatch( listenerCount );
        final List< BasicTestsInvocationHandler > nonExceptionThrowingBtihs = new CopyOnWriteArrayList<>();
        final List< BasicTestsInvocationHandler > listenerBtihs = new CopyOnWriteArrayList<>();
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        strategy.m_getDestThrows = true;
        final Method methodValidationCompleted = 
                ReflectUtil.getMethod( TapeMoveListener.class, "validationCompleted" );
        final Method methodMoveSucceeded = 
                ReflectUtil.getMethod( TapeMoveListener.class, "moveSucceeded" );
        final Method methodMoveFailed = 
                ReflectUtil.getMethod( TapeMoveListener.class, "moveFailed" );
        for ( int i = 0; i < listenerCount; ++i )
        {
            final boolean throwsException = ( 0 == i % 2 );
            final ListenerAdder listenerAdder = new ListenerAdder( latch, strategy, throwsException );
            listenerBtihs.add( listenerAdder.m_btih );
            if ( !throwsException )
            {
                nonExceptionThrowingBtihs.add( listenerAdder.m_btih );
            }
            SystemWorkPool.getInstance().submit( listenerAdder );
        }

        TestUtil.sleep( 1 );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                strategy.getDest(
                        666, 
                        tape,
                        InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, null ) );
            }
        } );
        latch.await();
        
        for ( final BasicTestsInvocationHandler btih : listenerBtihs )
        {
            assertEquals(1,  btih.getMethodCallCount(methodValidationCompleted), "Listener shoulda been notified of validationCompleted event no matter what.");
            assertEquals(0,  btih.getMethodCallCount(methodMoveFailed), "Listener should notta been notified of failure event since no failure occurred.");
        }
        for ( final BasicTestsInvocationHandler btih : nonExceptionThrowingBtihs )
        {
            assertEquals(1,  btih.getMethodCallCount(methodValidationCompleted), "Listener shoulda been notified of validationCompleted event no matter what.");
            final Object expected = tape.getId();
            assertEquals(expected, btih.getMethodInvokeData( methodValidationCompleted ).get( 0 ).getArgs().get( 0 ), "Validation completed event shoulda been a failure.");
            assertNotNull(btih.getMethodInvokeData( methodValidationCompleted ).get( 0 ).getArgs().get( 1 ), "Validation completed event shoulda been a failure.");
            assertEquals(0,  btih.getMethodCallCount(methodMoveSucceeded), "Listener should notta been notified of success event since no success occurred.");
        }
    }
    
    @Test
    public void 
    testAddedListenerReceivesSuccessNotificationsRegardlessAsToWhenListenerWasAddedOrOtherListeners()
            throws InterruptedException
    {
        final int listenerCount = 100;
        final CountDownLatch latch = new CountDownLatch( listenerCount );
        final List< BasicTestsInvocationHandler > nonExceptionThrowingBtihs = new CopyOnWriteArrayList<>();
        final List< BasicTestsInvocationHandler > listenerBtihs = new CopyOnWriteArrayList<>();
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        final Method methodValidationCompleted = 
                ReflectUtil.getMethod( TapeMoveListener.class, "validationCompleted" );
        final Method methodMoveSucceeded = 
                ReflectUtil.getMethod( TapeMoveListener.class, "moveSucceeded" );
        final Method methodMoveFailed = 
                ReflectUtil.getMethod( TapeMoveListener.class, "moveFailed" );
        for ( int i = 0; i < listenerCount; ++i )
        {
            final boolean throwsException = ( 0 == i % 2 );
            final ListenerAdder listenerAdder = new ListenerAdder( latch, strategy, throwsException );
            listenerBtihs.add( listenerAdder.m_btih );
            if ( !throwsException )
            {
                nonExceptionThrowingBtihs.add( listenerAdder.m_btih );
            }
            SystemWorkPool.getInstance().submit( listenerAdder );
        }

        TestUtil.sleep( 1 );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        strategy.getDest(
                666, 
                tape,
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, null ) );
        strategy.moveSucceeded();
        latch.await();
        
        for ( final BasicTestsInvocationHandler btih : listenerBtihs )
        {
            assertEquals(1,  btih.getMethodCallCount(methodValidationCompleted), "Listener shoulda been notified of validationCompleted event no matter what.");
            assertEquals(0,  btih.getMethodCallCount(methodMoveFailed), "Listener should notta been notified of failure event since no failure occurred.");
        }
        for ( final BasicTestsInvocationHandler btih : nonExceptionThrowingBtihs )
        {
            assertEquals(1,  btih.getMethodCallCount(methodValidationCompleted), "Listener shoulda been notified of validationCompleted event no matter what.");
            final Object expected = tape.getId();
            assertEquals(expected, btih.getMethodInvokeData( methodValidationCompleted ).get( 0 ).getArgs().get( 0 ), "Validation completed event shoulda been a success.");
            assertNull(btih.getMethodInvokeData( methodValidationCompleted ).get( 0 ).getArgs().get( 1 ), "Validation completed event shoulda been a success.");
            assertEquals(1,  btih.getMethodCallCount(methodMoveSucceeded), "Listener shoulda been notified of success event since success occurred.");
        }
    }
    
    @Test
    public void 
    testAddedListenerReceivesFailureNotificationsRegardlessAsToWhenListenerWasAddedOrOtherListeners()
            throws InterruptedException
    {
        final int listenerCount = 100;
        final CountDownLatch latch = new CountDownLatch( listenerCount );
        final List< BasicTestsInvocationHandler > nonExceptionThrowingBtihs = new CopyOnWriteArrayList<>();
        final List< BasicTestsInvocationHandler > listenerBtihs = new CopyOnWriteArrayList<>();
        final ConcreteTapeMoveStrategy strategy = new ConcreteTapeMoveStrategy();
        final Method methodValidationCompleted = 
                ReflectUtil.getMethod( TapeMoveListener.class, "validationCompleted" );
        final Method methodMoveSucceeded = 
                ReflectUtil.getMethod( TapeMoveListener.class, "moveSucceeded" );
        final Method methodMoveFailed = 
                ReflectUtil.getMethod( TapeMoveListener.class, "moveFailed" );
        for ( int i = 0; i < listenerCount; ++i )
        {
            final boolean throwsException = ( 0 == i % 2 );
            final ListenerAdder listenerAdder = new ListenerAdder( latch, strategy, throwsException );
            listenerBtihs.add( listenerAdder.m_btih );
            if ( !throwsException )
            {
                nonExceptionThrowingBtihs.add( listenerAdder.m_btih );
            }
            SystemWorkPool.getInstance().submit( listenerAdder );
        }

        TestUtil.sleep( 1 );
        final Tape tape = BeanFactory.newBean( Tape.class );
        tape.setId( UUID.randomUUID() );
        strategy.getDest(
                666, 
                tape,
                InterfaceProxyFactory.getProxy( TapeEnvironmentManager.class, null ) );
        strategy.moveFailed( tape );
        latch.await();
        
        for ( final BasicTestsInvocationHandler btih : listenerBtihs )
        {
            assertEquals(1,  btih.getMethodCallCount(methodValidationCompleted), "Listener shoulda been notified of validationCompleted event no matter what.");
            assertEquals(0,  btih.getMethodCallCount(methodMoveSucceeded), "Listener should notta been notified of success event since no success occurred.");
        }
        for ( final BasicTestsInvocationHandler btih : nonExceptionThrowingBtihs )
        {
            assertEquals(1,  btih.getMethodCallCount(methodValidationCompleted), "Listener shoulda been notified of validationCompleted event no matter what.");
            assertEquals(1,  btih.getMethodCallCount(methodMoveFailed), "Listener shoulda been notified of failure event since failure occurred.");
        }
    }
    
    
    private final static class ListenerAdder implements Runnable
    {
        private ListenerAdder( 
                final CountDownLatch latch, 
                final TapeMoveStrategy strategy, 
                final boolean throwsException )
        {
            m_latch = latch;
            m_strategy = strategy;

            m_btih = new BasicTestsInvocationHandler( 
                    new InvocationHandler()
                    {
                        public Object invoke( Object proxy, Method method, Object[] args )
                                throws Throwable
                        {
                            if ( method.getName().equals( "equals" ) )
                            {
                                if ( proxy == args[ 0 ] )
                                {
                                    return Boolean.TRUE;
                                }
                                return Boolean.FALSE;
                            }
                            if ( method.getName().equals( "toString" ) )
                            {
                                return "MockListener-" + BaseTapeMoveStrategy_Test.class.getSimpleName();
                            }
                            if ( throwsException )
                            {
                                throw new RuntimeException( "I like to misbehave and throw exceptions." );
                            }
                            return null;
                        }
                    } );
        }
        
        public void run()
        {
            if ( 0 < RANDOM.nextInt( 2 ) )
            {
                TestUtil.sleep( RANDOM.nextInt( 50 ) );
            }
            try
            {
                final TapeMoveListener listener = 
                        InterfaceProxyFactory.getProxy( TapeMoveListener.class, m_btih );
                m_strategy.addListener( listener );
                while ( 0 == RANDOM.nextInt( 5 ) )
                {
                    m_strategy.addListener( listener );
                }
            }
            finally
            {
                m_latch.countDown();
            }
        }
        
        private final CountDownLatch m_latch;
        private final TapeMoveStrategy m_strategy;
        private final BasicTestsInvocationHandler m_btih;
        private final static SecureRandom RANDOM = new SecureRandom();
    } // end inner class def
    
    
    private final static class ConcreteTapeMoveStrategy extends BaseTapeMoveStrategy
    {
        @Override
        protected int getDest()
        {
            m_getDestCallCount.incrementAndGet();
            if ( m_getDestThrows )
            {
                throw new IllegalStateException( "Oops." );
            }
            return 999;
        }

        @Override
        protected void commitMove()
        {
            m_commitMoveCallCount.incrementAndGet();
        }

        @Override
        protected void rollbackMove()
        {
            m_rollbackMoveCallCount.incrementAndGet();
        }
        
        private boolean m_getDestThrows;
        private final AtomicInteger m_getDestCallCount = new AtomicInteger();
        private final AtomicInteger m_commitMoveCallCount = new AtomicInteger();
        private final AtomicInteger m_rollbackMoveCallCount = new AtomicInteger();

        @Override
        public List<TapeDrive> getAssociatedDrives() {
            return Collections.emptyList();
        }
    } // end inner class def



}
