/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.mock.InterfaceProxyFactory;

public final class BeanFactory
{
    private BeanFactory()
    {
        // singleton
    }
    
    
    public static < B > Class< B > getType( final Class< B > clazz )
    {
        @SuppressWarnings( "unchecked" )
        final Class< B > retval = (Class< B >)BEAN_TYPES.get( clazz );
        return retval;
    }
    
    
    public static < B > B newBean( final Class< B > clazz )
    {
        return newBean( clazz, new HashMap< String, Object >() );
    }
    
    
    public static < B > B newBean( final Class< B > clazz, final Map< String, Object > initialPropValues )
    {
        try
        {
            final Constructor< ? > constructor;
            if ( clazz.isInterface() )
            {
                constructor = PROXIABLE_BEAN_CONSTRUCTORS.get( clazz );
            }
            else
            {
                constructor = NON_PROXIABLE_BEAN_CONSTRUCTORS.get( clazz );
            }
            
            if ( null == constructor )
            {
                final Map< String, Object > allInitialPropValues =
                        BeanUtils.getDefaultBeanPropValues( clazz, true );
                allInitialPropValues.putAll( initialPropValues );
                return InterfaceProxyFactory.getProxy(
                        clazz,
                        new BeanInvocationHandler( clazz, allInitialPropValues ) );
            }
        
            @SuppressWarnings( "unchecked" )
            final B retval = (B)constructor.newInstance();
            populateInitialPropValues( clazz, retval, initialPropValues );
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to create a " + clazz.getSimpleName(), ex );
        }
    }
    
    
    private static void populateInitialPropValues(
            final Class< ? > clazz,
            final Object bean, 
            Map< String, Object > initialPropValues )
    {
        initialPropValues = new HashMap<>( BeanUtils.getDefaultBeanPropValues( clazz, false ) );
        initialPropValues.putAll( initialPropValues );
        
        for ( final Map.Entry< String, Object > e : initialPropValues.entrySet() )
        {
            if ( null == e.getValue() )
            {
                continue;
            }
            
            try
            {
                final Method writer = BeanUtils.getWriter( clazz, e.getKey() );
                writer.setAccessible( true );
                writer.invoke(
                        bean, 
                        new Object [] { CollectionFactory.getDefensiveCopy( e.getValue() ) } );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to set " + e.getKey() + " bean property.", ex );
            }
        }
    }
    
    
    private final static class ProxiableBeanConstructorCacheResultProvider 
        implements CacheResultProvider< Class< ? >, Constructor< ? > >
    {
        public Constructor< ? > generateCacheResultFor( final Class< ? > param )
        {
            if ( null == param.getAnnotation( ConcreteImplementation.class ) )
            {
                return null;
            }
            
            try
            {
                final Constructor< ? > retval = 
                        param.getAnnotation( ConcreteImplementation.class ).value().getDeclaredConstructor();
                retval.setAccessible( true );
                return retval;
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
    } // end inner class def
    
    
    private final static class NonProxiableBeanConstructorCacheResultProvider 
        implements CacheResultProvider< Class< ? >, Constructor< ? > >
    {
        public Constructor< ? > generateCacheResultFor( final Class< ? > param )
        {
            try
            {
                final Constructor< ? > retval = param.getDeclaredConstructor();
                retval.setAccessible( true );
                return retval;
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
    } // end inner class def
    
    
    private final static class BeanTypeCacheResultProvider
        implements CacheResultProvider< Class< ? >, Class< ? > >
    {
        public Class< ? > generateCacheResultFor( final Class< ? > clazz )
        {
            Validations.verifyNotNull( "Class", clazz );
            if ( 1 == clazz.getInterfaces().length 
                    && null != clazz.getInterfaces()[ 0 ].getAnnotation( ConcreteImplementation.class ) )
            {
                return BeanFactory.getType( clazz.getInterfaces()[ 0 ] );
            }
            
            return InterfaceProxyFactory.getType( clazz );
        }
    } // end inner class def
    
    
    private final static StaticCache< Class< ? >, Constructor< ? > > PROXIABLE_BEAN_CONSTRUCTORS =
            new StaticCache<>( new ProxiableBeanConstructorCacheResultProvider() );
    private final static StaticCache< Class< ? >, Constructor< ? > > NON_PROXIABLE_BEAN_CONSTRUCTORS =
            new StaticCache<>( new NonProxiableBeanConstructorCacheResultProvider() );
    private final static StaticCache< Class< ? >, Class< ? > > BEAN_TYPES =
            new StaticCache<>( new BeanTypeCacheResultProvider() );
}
