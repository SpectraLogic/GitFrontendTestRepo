/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;

/**
 * An invocation handler that keeps track of call metrics so that basic tests (like method call
 * counts) can be performed.
 */
public final class BasicTestsInvocationHandler implements InvocationHandler
{
    public BasicTestsInvocationHandler( final InvocationHandler decoratedInvocationHandler )
    {
        m_decoratedIh = ( null == decoratedInvocationHandler ) ?
                NullInvocationHandler.getInstance() 
                : decoratedInvocationHandler;
    }
    
    
    @Override
    public Object invoke( 
            final Object proxy, 
            final Method method,
            final Object[] args ) throws Throwable
    {
        synchronized ( m_methodInvokeDatas )
        {
            m_methodInvokeDatas.add( 
                    new MethodInvokeData( proxy, method, CollectionFactory.toList( args ) ) );
        }
        return m_decoratedIh.invoke( proxy, method, args );
    }
    
    
    public void eventuallyVerifyMethodInvocations( 
            final Map< Method, Integer > expectedCallCounts,
            final int timeoutInMillis )
    {
        final Duration d = new Duration();
        while ( true )
        {
            try
            {
                verifyMethodInvocations( expectedCallCounts );
                return;
            }
            catch ( final RuntimeException ex )
            {
                if ( d.getElapsedMillis() > timeoutInMillis )
                {
                    throw ex;
                }
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( final InterruptedException ex1 )
                {
                    throw new RuntimeException( ex1 );
                }
            }
        }
    }
    
    
    public void verifyMethodInvocations( final Map< Method, Integer > expectedCallCounts )
    {
        final Map< Method, Integer > actualCallCounts = new HashMap<>();
        for ( final MethodInvokeData data : getMethodInvokeData() )
        {
            if ( actualCallCounts.containsKey( data.getMethod() ) )
            {
                actualCallCounts.put( 
                        data.getMethod(),
                        Integer.valueOf( 1 + actualCallCounts.get( data.getMethod() ).intValue() ) );
            }
            else
            {
                actualCallCounts.put( data.getMethod(), Integer.valueOf( 1 ) );
            }
        }
        
        final Set< String > violations = new HashSet<>();
        for ( final Method m : expectedCallCounts.keySet() )
        {
            Integer actualCallCount = actualCallCounts.get( m );
            final Integer expectedCallCount = expectedCallCounts.get( m );
            if ( null == actualCallCount )
            {
                actualCallCount = Integer.valueOf( 0 );
            }
            if ( null != expectedCallCount )
            {
                if ( !expectedCallCount.equals( actualCallCount ) )
                {
                    violations.add( 
                            "Expected " + m + " to be called " + expectedCallCount 
                            + " times, but it was called " + actualCallCount + " times." );
                }
            }
        }
        
        final Set< Method > unexpectedMethodsCalled = actualCallCounts.keySet();
        unexpectedMethodsCalled.removeAll( expectedCallCounts.keySet() );
        for ( final Method m : unexpectedMethodsCalled )
        {
            violations.add( "Did not expect " + m + " to be called.");
        }
        
        if ( !violations.isEmpty() )
        {
            String msg = violations.size() + " violations:";
            for ( final String violation : violations )
            {
                msg += Platform.NEWLINE + violation;
            }
            throw new RuntimeException( msg );
        }
    }
    
    
    public int getMethodCallCount( final Method method )
    {
        return getMethodInvokeData( method ).size();
    }


    public int getMethodCallMatchingPredicateCount(final Method method, final Predicate<? super MethodInvokeData> predicate )
    {
        return (int) getMethodInvokeData(method).stream().filter(predicate).count();
    }

    
    public List< MethodInvokeData > getMethodInvokeData( final Method method )
    {
        Validations.verifyNotNull( "Method", method );
        
        final List< MethodInvokeData > retval = new ArrayList<>();
        for ( final MethodInvokeData data : getMethodInvokeData() )
        {
            if ( method.equals( data.getMethod() ) )
            {
                retval.add( data );
            }
        }
        
        return retval;
    }
    
    
    public List< MethodInvokeData > getMethodInvokeData()
    {
        synchronized ( m_methodInvokeDatas )
        {
            return new ArrayList<>( m_methodInvokeDatas );
        }
    }
    
    
    public int getTotalCallCount()
    {
        synchronized ( m_methodInvokeDatas )
        {
            return m_methodInvokeDatas.size();
        }
    }
    
    
    public void reset()
    {
        synchronized ( m_methodInvokeDatas )
        {
            m_methodInvokeDatas.clear();
        }
    }
    
    
    public class MethodInvokeData
    {
        private MethodInvokeData( 
                final Object proxy, 
                final Method method,
                final List< Object > args )
        {
            m_proxy = proxy;
            m_method = method;
            m_args = args;
        }
        
        
        public Object getProxy()
        {
            return m_proxy;
        }
        
        
        public Method getMethod()
        {
            return m_method;
        }
        
        
        public List< Object > getArgs()
        {
            return new ArrayList<>( m_args );
        }
        
        
        private final Object m_proxy;
        private final Method m_method;
        private final List< Object > m_args;
    } // end inner class def
    
    
    private final List< MethodInvokeData > m_methodInvokeDatas = new ArrayList<>();
    private final InvocationHandler m_decoratedIh;
}
