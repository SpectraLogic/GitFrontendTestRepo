/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

final class BeanInvocationHandler implements InvocationHandler
{
    BeanInvocationHandler( final Class< ? > clazz, final Map< String, Object > initialPropValues )
    {
        Validations.verifyNotNull( "Class", clazz );
        Validations.verifyNotNull( "Initial prop values", initialPropValues );
        m_clazz = clazz;
        if ( !SimpleBeanSafeToProxy.class.isAssignableFrom( m_clazz ) )
        {
            throw new IllegalArgumentException(
                    m_clazz.getSimpleName() + " is not marked as " 
                    + SimpleBeanSafeToProxy.class.getSimpleName() );
        }
        
        for ( final Map.Entry< String, Object > e : initialPropValues.entrySet() )
        {
            m_beanPropertyValues.put(
                    e.getKey(),
                    CollectionFactory.getDefensiveCopy( e.getValue() ) );
        }
    }
    

    public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
    {
        final BeanMethodInvocationHandler bmih = method.getAnnotation( BeanMethodInvocationHandler.class );
        if ( null != bmih )
        {
            if ( !BEAN_METHOD_INVOCATION_HANDLERS.containsKey( bmih.value() ) )
            {
                synchronized ( BEAN_METHOD_INVOCATION_HANDLERS )
                {
                    if ( !BEAN_METHOD_INVOCATION_HANDLERS.containsKey( bmih.value() ) )
                    {
                        BEAN_METHOD_INVOCATION_HANDLERS.put( bmih.value(), bmih.value().newInstance() );
                    }
                }
            }
            
            /*
             * Note the lack of synchronization when invoking the invocation handler. 
             * This is intentional for performance reasons.  The handler is given the 
             * proxy, method, and args, so it should almost always be stateless, making 
             * it inherently thread-safe.
             */
            return BEAN_METHOD_INVOCATION_HANDLERS.get( bmih.value() ).invoke( proxy, method, args );
        }
        
        /*
         * Object methods
         */
        if ( method.getName().equals( "equals" ) )
        {
            return invokedEquals( proxy, args[ 0 ] );
        }
        if ( method.getName().equals( "hashCode" ) )
        {
            return invokedHashCode();
        }
        if ( method.getName().equals( "toString" ) )
        {
            return invokedToString();
        }
        
        /*
         * Bean property methods
         */
        if ( BeanUtils.isReader( m_clazz, method ) )
        {
            return invokedBeanGetter( BeanUtils.getPropertyName( m_clazz, method ) );
        }
        if ( BeanUtils.isWriter( m_clazz, method ) )
        {
            invokedBeanSetter( BeanUtils.getPropertyName( m_clazz, method ), args[ 0 ] );
            if ( void.class == method.getReturnType() )
            {
                return null;
            }
            return proxy;
        }
        
        /*
         * Unsupported methods
         */
        throw new UnsupportedOperationException( "No code for " + method );
    }
    
    
    private Boolean invokedEquals( final Object proxy, final Object other )
    {
        return Boolean.valueOf( proxy == other );
    }
    
    
    private Integer invokedHashCode()
    {
        return Integer.valueOf( hashCode() );
    }
    
    
    synchronized private Object invokedBeanGetter( final String prop )
    {
        if ( !m_beanPropertyValues.containsKey( prop ) )
        {
            m_beanPropertyValues.put(
                    prop,
                    BeanUtils.getDefaultBeanPropValue( m_clazz, prop ) );
        }
        return CollectionFactory.getDefensiveCopy( m_beanPropertyValues.get( prop ) );
    }
    
    
    synchronized private void invokedBeanSetter( final String prop, final Object value )
    {
        m_beanPropertyValues.put( prop, CollectionFactory.getDefensiveCopy( value ) );
    }
    
    
    private Object invokedToString()
    {
        final Map< String, Object > sanitizedBeanPropertyValues = new HashMap<>( m_beanPropertyValues );
        for ( final String prop : m_beanPropertyValues.keySet() )
        {
            final Method reader = BeanUtils.getReader( m_clazz, prop );
            if ( null == reader )
            {
                continue;
            }
            if ( null != reader.getAnnotation( Secret.class ) )
            {
                sanitizedBeanPropertyValues.remove( prop );
            }
        }
        return m_clazz.getName() + "@" + this.getClass().getSimpleName() + sanitizedBeanPropertyValues;
    }

    
    private final Class< ? > m_clazz;
    private final Map< String, Object > m_beanPropertyValues = new HashMap<>();
    private final static Map< Class< ? >, InvocationHandler > BEAN_METHOD_INVOCATION_HANDLERS = 
            new HashMap<>();
}
