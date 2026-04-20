/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.reflect.Method;
import java.util.Set;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.reflect.ReflectUtil;


/**
 * Always use this over Apache's PropertyUtils.copyProperties since Apache does
 * not perform a deep copy.  This does, and will fail if a deep copy is not 
 * possible. <br><br>
 * 
 * A deep copy is performed whenever a bean property value is itself 
 * {@link SimpleBeanSafeToProxy}.  Since this class does not and should 
 * not have knowledge of how to instantiate such a value, the deep copy will 
 * fail if such instantiation would be required (e.g. the source has a non-null 
 * value and the destination has a null value).  In such cases, the client is 
 * responsible for properly instantiating the bean(s) on the destination.  Note 
 * that it would be incorrect to simply copy the source's value over to the 
 * destination since the nested bean instance usually needs to be independent 
 * between the source and destination.
 *
 */
public final class BeanCopier
{
    private BeanCopier()
    {
        // singleton
    }
    
    
    public static < B extends SimpleBeanSafeToProxy > B createCopy( final Class< B > type, final B source )
    {
        final B destination = BeanFactory.newBean( type );
        copy( destination, source );
        return destination;
    }
    
    
    /**
     * Always use this over Apache's PropertyUtils.copyProperties since Apache does
     * not perform a deep copy.  This does, and will fail if a deep copy is not 
     * possible. <br><br>
     * 
     * A deep copy is performed whenever a bean property value is itself 
     * {@link SimpleBeanSafeToProxy}.  Since this class does not and should 
     * not have knowledge of how to instantiate such a value, the deep copy will 
     * fail if such instantiation would be required (e.g. the source has a non-null 
     * value and the destination has a null value).  In such cases, the client is 
     * responsible for properly instantiating the bean(s) on the destination.  Note 
     * that it would be incorrect to simply copy the source's value over to the 
     * destination since the nested bean instance usually needs to be independent 
     * between the source and destination.
     */
    public static void copy( final Object destination, final Object source )
    {
        INSTANCE.copyInternal( destination, source );
    }
    
    
    private void copyInternal( final Object destination, final Object source )
    {
        if ( null == source )
        {
            throw new IllegalArgumentException( "Source cannot be null." ); 
        }
        if ( null == destination )
        {
            throw new IllegalArgumentException( "Destination cannot be null." ); 
        }
        
        final Class< ? > clazz = source.getClass();
        final Set< String > properties = BeanUtils.getPropertyNames( clazz );
        for ( String prop : properties )
        {
            final CopyParams params = new CopyParams( prop, source, destination );
            if ( null == params.m_reader 
                    || params.m_originalDestinationValue == params.m_originalSourceValue )
            {
                continue;
            }

            final Class< ? > returnType = params.m_reader.getReturnType();
            if ( SimpleBeanSafeToProxy.class.isAssignableFrom( returnType ) )
            {
                copyNonArrayNestedBeanProperty( params );
            }
            else if ( returnType.isArray() && SimpleBeanSafeToProxy.class.isAssignableFrom( 
                    returnType.getComponentType() ) )
            {
                copyArrayNestedBeanProperty( params );
            }
            else
            {
                copyRegularProperty( params );
            }
        }
    }
    
    
    private void copyRegularProperty( final CopyParams params )
    {
        if ( null == params.m_writer )
        {
            return;
        }
        
        writeValueToDestinationBean( params, params.m_originalSourceValue );
    }
    
    
    private void copyNonArrayNestedBeanProperty( final CopyParams params )
    {
        if ( null == params.m_originalSourceValue && null == params.m_originalDestinationValue )
        {
            return;
        }

        verifyDestinationNonNullIfSourceIsNonNull( params );
        
        if ( null == params.m_originalSourceValue )
        {
            writeValueToDestinationBean( params, null );
            return;
        }
        
        copy( params.m_originalDestinationValue, params.m_originalSourceValue );
    }
    
    
    private void copyArrayNestedBeanProperty( final CopyParams params )
    {
        if ( null == params.m_originalSourceValue && null == params.m_originalDestinationValue )
        {
            return;
        }
        
        verifyDestinationNonNullIfSourceIsNonNull( params );
        
        if ( null == params.m_originalSourceValue )
        {
            writeValueToDestinationBean( params, null );
            return;
        }
        
        final Object [] sourceValue = (Object[])params.m_originalSourceValue;
        final Object [] destinationValue = (Object[])params.m_originalDestinationValue;
        
        verifyArrayLengthsMatch( params, sourceValue, destinationValue );
        
        for ( int i = 0; i < sourceValue.length; ++i )
        {
            copy( destinationValue[ i ], sourceValue[ i ] );
        }
    }
    
    
    private void verifyArrayLengthsMatch(
            final CopyParams params,
            final Object [] sourceValue,
            final Object [] destinationValue )
    {
        if ( sourceValue.length != destinationValue.length )
        {
            throw new IllegalArgumentException( new StringBuilder( 200 )
                .append( getFailureMessagePrefix( params ) )
                .append( "The source value array length (" ) 
                .append( sourceValue.length )
                .append( " ) does not match the destination value array length (" ) 
                .append( destinationValue.length )
                .append( ").  The client is responsible for setting the " ) 
                .append( "appropriate-length array on the destination since " ) 
                .append( this.getClass().getSimpleName() )
                .append( " does not and should not have the " ) 
                .append( "knowledge necessary to do so." ) 
                .toString() );
        }
    }
    
    
    private void verifyDestinationNonNullIfSourceIsNonNull( final CopyParams params )
    {
        if ( null != params.m_originalSourceValue && null == params.m_originalDestinationValue )
        {
            throw new IllegalArgumentException( new StringBuilder( 200 )
                .append( getFailureMessagePrefix( params ) )
                .append( "The client is responsible for instantiating " ) 
                .append( "the nested bean on the destination since " ) 
                .append( this.getClass().getSimpleName() )
                .append( " does not and should not have the " ) 
                .append( "knowledge necessary to do so." ) 
                .toString() );
        }
    }
    
    
    private String getFailureMessagePrefix( final CopyParams params )
    {
        return new StringBuilder( 200 )
            .append( "For property " ) 
            .append( params.m_prop )
            .append( " on " ) 
            .append( params.m_destinationBeanType.getSimpleName() )
            .append( ", source value is " ) 
            .append( params.m_originalSourceValue )
            .append( " and destination value is " ) 
            .append( params.m_originalDestinationValue )
            .append( ".  " )
            .toString();
    }
    
    
    private void writeValueToDestinationBean( 
            final CopyParams params,
            Object value )
    {
        try
        {
            if ( null != value 
                    && value.getClass().isEnum()
                    && params.m_writer.getParameterTypes()[ 0 ].isEnum()
                    && params.m_writer.getParameterTypes()[ 0 ] != value.getClass() )
            {
                value = ReflectUtil.enumValueOf( params.m_writer.getParameterTypes()[ 0 ], value.toString() );
            }
            params.m_writer.invoke( params.m_destination, value );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( new StringBuilder( 200 )
                .append( getFailureMessagePrefix( params ) )
                .append( ".  Failed to invoked writer." ) 
                .toString(), ex );
        }
    }
    
    
    private final static class CopyParams
    {
        private CopyParams(
                final String prop,
                final Object source,
                final Object destination )
        {
            m_prop = prop;
            m_source = source;
            m_destination = destination;
            m_sourceBeanType = BeanFactory.getType( m_source.getClass() );
            m_destinationBeanType = BeanFactory.getType( m_destination.getClass() );
            
            m_reader = BeanUtils.getReader( m_sourceBeanType, m_prop );
            m_writer = BeanUtils.getWriter( m_destinationBeanType, m_prop );
            
            if ( null == m_reader || null == m_writer )
            {
                m_originalSourceValue = null;
                m_originalDestinationValue = null;
            }
            else
            {
                m_writer.setAccessible( true );
                m_reader.setAccessible( true );
                
                try
                {
                    m_originalSourceValue = m_reader.invoke( m_source );
                    m_originalDestinationValue = 
                            BeanUtils.getReader( m_destinationBeanType, m_prop ).invoke( m_destination );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( "Failed to invoke reader.", ex ); 
                }
            }
        }
        
        
        private final String m_prop;
        private final Object m_source;
        private final Object m_destination;
        private final Class< ? > m_sourceBeanType;
        private final Class< ? > m_destinationBeanType;
        
        private final Method m_reader;
        private final Method m_writer;
        
        private final Object m_originalSourceValue;
        private final Object m_originalDestinationValue;
    } // end inner class def
    
    
    private final static BeanCopier INSTANCE = new BeanCopier();
}
