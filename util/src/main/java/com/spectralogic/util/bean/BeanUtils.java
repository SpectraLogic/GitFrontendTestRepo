/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.reflect.Method;
import java.util.*;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;

/**
 * Utility methods for manipulating Java Bean objects.
 */
public final class BeanUtils
{
    public static Set< String > getPropertyNames( final Class< ? > clazz )
    {
        Validations.verifyNotNull( "Class", clazz );
        final Set< String > retval = new HashSet<>();
        retval.addAll( CACHE.getBeanInfo( clazz ).getPropNameToReaderMethodMap().keySet() );
        retval.addAll( CACHE.getBeanInfo( clazz ).getPropNameToWriterMethodMap().keySet() );
        return retval;
    }
    
    
    public static boolean hasPropertyName( final Class< ? > clazz, final String propertyName )
    {
        Validations.verifyNotNull( "Class", clazz );
        Validations.verifyNotNull( "Property name", propertyName );
        if ( CACHE.getBeanInfo( clazz ).getPropNameToReaderMethodMap().keySet().contains( propertyName ) )
        {
            return true;
        }
        return CACHE.getBeanInfo( clazz )
                    .getPropNameToWriterMethodMap()
                    .keySet()
                    .contains( propertyName );
    }
    
    
    public static String getPropertyName( final Class< ? > clazz, final Method beanMethod )
    {
        final String retval = BeanInfo.getBeanPropertyNameForMethod( beanMethod );
        if ( CACHE.getBeanInfo( clazz ).getReaderMethodToPropNameMap().containsKey( beanMethod )
                || CACHE.getBeanInfo( clazz ).getWriterMethodToPropNameMap().containsKey( beanMethod ) )
        {
            return retval;
        }
        return null;
    }
    
    
    public static boolean isReader( final Class< ? > clazz, final Method method )
    {
        Validations.verifyNotNull( "Method", method );
        return CACHE.getBeanInfo( clazz ).getReaderMethodToPropNameMap().containsKey( method );
    }
    
    
    public static void verifyReaderReturnType(
            final Class< ? > clazz, 
            final String propertyName, 
            final Class< ? > expectedReturnType )
    {
        Validations.verifyNotNull( "Expected return type", expectedReturnType );
        
        final Method reader = getReader( clazz, propertyName );
        if ( null == reader )
        {
            throw new IllegalArgumentException( "No reader for " + clazz + "." + propertyName );
        }
        if ( !ReflectUtil.toNonPrimitiveType( expectedReturnType ).isAssignableFrom(
                ReflectUtil.toNonPrimitiveType( reader.getReturnType() ) ) )
        {
            throw new IllegalArgumentException( 
                    "Expected reader for " + clazz + "." + propertyName + " to be of type "
                     + expectedReturnType + ", but was " + reader.getReturnType() + "." );
        }
    }
    
    
    public static Method getReader( final Class< ? > clazz, final String propertyName )
    {
        Validations.verifyNotNull( "Property name", propertyName );
        return CACHE.getBeanInfo( clazz ).getPropNameToReaderMethodMap().get( propertyName );
    }
    
    
    public static boolean isWriter( final Class< ? > clazz, final Method method )
    {
        Validations.verifyNotNull( "Method", method );
        return CACHE.getBeanInfo( clazz ).getWriterMethodToPropNameMap().containsKey( method );
    }
    
    
    public static Method getWriter( final Class< ? > clazz, final String propertyName )
    {
        Validations.verifyNotNull( "Property name", propertyName );
        return CACHE.getBeanInfo( clazz ).getPropNameToWriterMethodMap().get( propertyName );
    }
    
    
    public static Set< String > getColumnIndexes( final Class< ? > clazz )
    {
        return CACHE.getBeanInfo( clazz )
                    .getColumnIndexes();
    }
    
    
    public static BeanSQLOrdering getSqlOrdering( final Class< ? > clazz )
    {
        return CACHE.getBeanInfo( clazz )
                    .getBeanSqlOrdering();
    }
    
    
    public static < T extends Identifiable > SortedSet< T > getSortedSet( final Class< T > clazz )
    {
        final Comparator< Object > comparator = CACHE.getBeanInfo( clazz ).getComparator();
        if ( null == comparator )
        {
            return null;
        }
        
        return new TreeSet<>( comparator );
    }
    
    
    public static < T extends Identifiable > Comparator< Object > getComparator( final Class< T > clazz )
    {
    	return CACHE.getBeanInfo( clazz ).getComparator();
    }
    
    
    public static < T extends Identifiable > Set< T > sort( final Set< T > unsortedBeans )
    {
        Validations.verifyNotNull( "Unsorted beans", unsortedBeans );
        if ( unsortedBeans.isEmpty() )
        {
            return new TreeSet<>();
        }
        
        @SuppressWarnings( "unchecked" )
        final Class< T > clazz = (Class< T >)unsortedBeans.iterator().next().getClass();
        final SortedSet< T > retval = getSortedSet( clazz );
        if ( null == retval )
        {
            return unsortedBeans;
        }
        
        retval.addAll( unsortedBeans );
        return retval;
    }
    

    /**
     * Note that this method does not preserve the order of the input collection.
     * 
     * @return Set <property value>
     */
    public static < T > Set< T > extractPropertyValues(
            final Collection< ? > beans, 
            final String propertyToExtract )
    {
        Validations.verifyNotNull( "Beans", beans );
        for ( final Object o : beans )
        {
            if ( null == o )
            {
                throw new IllegalArgumentException( "Beans cannot contain null." );    
            }
        }
        if ( beans.isEmpty() )
        {
            return new HashSet<>();
        }
        
        final Method reader = BeanUtils.getReader( beans.iterator().next().getClass(), propertyToExtract );
        final Set< T > retval = new HashSet<>();
        for ( final Object bean : beans )
        {
            try
            {
                @SuppressWarnings( "unchecked" )
                final T propValue = (T)reader.invoke( bean );
                retval.add( propValue );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to invoke " + reader + " on " + bean + ".", ex );
            }
        }
        
        return retval;
    }
    
    
    /**
     * Note that this method does not preserve the order of the input collection.
     * 
     * @return {@code Map <{@link Identifiable#ID}, bean>}
     */
    public static < T extends Identifiable > Map< UUID, T > toMap( final Collection< T > beans )
    {
        final Map< UUID, ? > map = toMap( beans, null );
        @SuppressWarnings( "unchecked" )
        final Map< UUID, T > retval = (Map< UUID, T >)map;
        return retval;
    }
    
    
    /**
     * Note that this method does not preserve the order of the input collection.
     * 
     * @return {@code Map <{@link Identifiable#ID}, property value>}
     */
    public static < T extends Identifiable, V > Map< UUID, V > toMap(
            final Collection< T > beans,
            final String beanPropertyToExtract )
    {
        Validations.verifyNotNull( "Beans", beans );
        if ( beans.isEmpty() )
        {
            return new HashMap<>();
        }
        
        final Method reader = ( null == beanPropertyToExtract ) ? 
                null 
                : BeanUtils.getReader( beans.iterator().next().getClass(), beanPropertyToExtract );
        final Map< UUID, V > retval = new HashMap<>();
        for ( final T bean : beans )
        {
            if ( null == bean.getId() )
            {
                throw new IllegalArgumentException( "Bean has null id: " + bean );
            }
            
            final Object value;
            if ( null == reader )
            {
                value = bean;
            }
            else
            {
                try
                {
                    value = reader.invoke( bean );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( ex );
                }
            }
            
            @SuppressWarnings( "unchecked" )
            final V castedValue = (V)value;
            retval.put( bean.getId(), castedValue );
        }
        
        return retval;
    }


    public static < T extends Identifiable, V > Map< V, T > mapBeansByProperty(
            final Collection< T > beans,
            final String propertyToMapBy )
    {
        Validations.verifyNotNull( "Beans", beans );
        if ( beans.isEmpty() )
        {
            return new HashMap<>();
        }

        final Method reader = ( null == propertyToMapBy ) ?
                null
                : BeanUtils.getReader( beans.iterator().next().getClass(), propertyToMapBy );
        final Map< V, T > retval = new HashMap<>();
        for ( final T bean : beans )
        {
            if ( null == bean.getId() )
            {
                throw new IllegalArgumentException( "Bean has null id: " + bean );
            }

            final Object value;
            if ( null == reader )
            {
                value = bean;
            }
            else
            {
                try
                {
                    value = reader.invoke( bean );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( ex );
                }
            }

            @SuppressWarnings( "unchecked" )
            final V castedValue = (V)value;
            retval.put( castedValue, bean );
        }

        return retval;
    }


    public static <T extends SimpleBeanSafeToProxy> List<String> getChangedProps(final Class<T> clazz, final T old, final T latest) {
        final List<String> changedProps = new ArrayList<>();
        for ( final String prop : BeanUtils.getPropertyNames( clazz ) )
        {
            try
            {
                final Method reader = BeanUtils.getReader( clazz, prop );
                final Object oldValue = reader.invoke( old );
                final Object newValue = reader.invoke(latest);
                if ( ( null == oldValue ) != ( null == newValue )
                        || null != oldValue && !oldValue.equals( newValue ) )
                {
                    changedProps.add( prop );
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to check for equality of property " + prop + ".", ex );
            }
        }
        Collections.sort( changedProps );
        return changedProps;
    }
    
    
    static Map< String, Object > getDefaultBeanPropValues(
            final Class< ? > clazz, 
            final boolean excludeConstantDefaultValues )
    {
        final Map< String, Object > retval = new HashMap<>();
        for ( final Map.Entry< String, DefaultBeanPropertyValueProvider > e 
                : CACHE.getBeanInfo( clazz ).getDefaultPropValueMap().entrySet() )
        {
            if ( excludeConstantDefaultValues 
                    && ConstantDefaultBeanPropertyValueProvider.class == e.getValue().getClass() )
            {
                continue;
            }
            retval.put( e.getKey(), e.getValue().getDefaultValue() );
        }
        return retval;
    }
    
    
    public static Object getDefaultBeanPropValue( final Class< ? > clazz, final String propertyName )
    {
        final DefaultBeanPropertyValueProvider retval=
                CACHE.getBeanInfo( clazz ).getDefaultPropValueMap().get( propertyName );
        return ( null == retval ) ? null : retval.getDefaultValue();
    }
    
    
    private final static BeanInfoCache CACHE = new BeanInfoCacheImpl();
}
