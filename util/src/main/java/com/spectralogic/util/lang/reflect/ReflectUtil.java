/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.reflect;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;

public final class ReflectUtil
{
    private ReflectUtil()
    {
        // singleton
    }
    
    
    public static < S extends Enum< S >, D extends Enum< D > > D convertEnum(
            final Class< S > source,
            final Class< D > destination,
            final S value )
    {
        final Map< Object, Object > map = getEnumConversionMap( source, destination );
        @SuppressWarnings( "unchecked" )
        final D retval = (D)map.get( value );
        return retval;
    }
    
    
    private static < S extends Enum< S >, D extends Enum< D > > 
    Map< Object, Object > getEnumConversionMap( final Class< S > source, final Class< D > destination )
    {
        synchronized ( ENUM_VALUE_CONV_MAP )
        {
            if ( !ENUM_VALUE_CONV_MAP.containsKey( source ) )
            {
                ENUM_VALUE_CONV_MAP.put( source, new HashMap< Class< ? >, Map< Object, Object > >() );
            }
            
            Map< Object, Object > retval = ENUM_VALUE_CONV_MAP.get( source ).get( destination );
            if ( null != retval )
            {
                return retval;
            }
            
            retval = new HashMap<>();
            for ( final Object s : source.getEnumConstants() )
            {
                final String textRequired = s.toString().replace( "_", "" ).toLowerCase();
                Object d = null;
                for ( final Object candidate : destination.getEnumConstants() )
                {
                    final String text = candidate.toString().replace( "_", "" ).toLowerCase();
                    if ( textRequired.equals( text ) )
                    {
                        if ( null != d )
                        {
                            throw new RuntimeException(
                                    "There are multiple candidates for: " + source.getName() + "." + s );
                        }
                        d = candidate;
                    }
                }
                if ( null == d )
                {
                    throw new RuntimeException( "No candidate found for: " + source.getName() + "." + s );
                }
                retval.put( s, d );
            }
            
            ENUM_VALUE_CONV_MAP.get( source ).put( destination, retval );
            return getEnumConversionMap( source, destination );
        }
    }
    
    
    public static Object[] toObjectArray( final Object array ) 
    {
        if ( null == array )
        {
            return null;
        }
        if ( !array.getClass().isArray() )
        {
            throw new IllegalArgumentException( "Not an array: " + array );
        }
        if ( !array.getClass().getComponentType().isPrimitive() )
        {
            return (Object[])array;
        }
        
        final int length = Array.getLength(array);
        final Object[] retval = new Object[ length ];
        for ( int i = 0; i < length; ++i )
        {
            retval[ i ] = Array.get( array, i );
        }
        
        return retval;
    }
    
    
    public static < T > T enumValueOf( final Class< T > enumType, final String valueInRawForm )
    {
        final String enumName = NamingConventionType.CONSTANT.convert( valueInRawForm );
        try
        {
            @SuppressWarnings( "unchecked" )
            final T retval = (T)ENUM_VALUE_OF.invoke( 
                    null,
                    enumType,
                    enumName );
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.BAD_REQUEST, 
                    "Failed to resolve '" + valueInRawForm + "' to a valid enum constant since '" + enumName 
                    + "' is not in the set of possible values: "
                    + CollectionFactory.toList( enumType.getEnumConstants() ), ex );
        }
    }
    
    
    public static Class< ? > toPrimitiveType( final Class< ? > type )
    {
        Validations.verifyNotNull( "Type", type );

        if ( NON_PRIMITIVE_TO_PRIMITIVE_MAP.containsKey( type ) )
        {
            return NON_PRIMITIVE_TO_PRIMITIVE_MAP.get( type );
        }
        
        return type;
    }
    
    
    public static Class< ? > toNonPrimitiveType( final Class< ? > type )
    {
        Validations.verifyNotNull( "Type", type );
        
        if ( void.class == type )
        {
            return void.class;
        }
        
        if ( !type.isPrimitive() )
        {
            return type;
        }

        if ( PRIMITIVE_TO_NON_PRIMITIVE_MAP.containsKey( type ) )
        {
            return PRIMITIVE_TO_NON_PRIMITIVE_MAP.get( type );
        }
        
        throw new UnsupportedOperationException( "No code written to support " + type );
    }
    
    
    public static Method getMethod( final Class< ? > clazz, final String methodName )
    {
        final Method retval = getMethodInternal( clazz, methodName );
        if ( null == retval )
        {
            throw new RuntimeException( "No candidates found for '" + methodName + "' on '" + clazz + "'." );
        }
        return retval;
    }
    
    
    private static Method getMethodInternal( final Class< ? > clazz, final String methodName )
    {
        if ( null == clazz )
        {
            throw new IllegalArgumentException( "Class cannot be null." );
        }
        if ( null == methodName )
        {
            throw new IllegalArgumentException( "Method name cannot be null." );
        }
        
        Method retval = null;
        for ( final Method m : clazz.getMethods() )
        {
            if ( m.getName().equals( methodName ) )
            {
                if ( null != retval )
                {
                    throw new RuntimeException( 
                            "Multiple candidates found for '" + methodName + "' on " + clazz + "'." );
                }
                retval = m;
            }
        }
        if ( null != retval )
        {
            return retval;
        }
        
        for ( final Method m : clazz.getDeclaredMethods() )
        {
            if ( m.getName().equals( methodName ) )
            {
                if ( null != retval )
                {
                    throw new RuntimeException( 
                            "Multiple candidates found for '" + methodName + "' on " + clazz + "'." );
                }
                retval = m;
            }
        }
        if ( null != retval )
        {
            return retval;
        }
        
        return getImplicitMethod(clazz, methodName );
    }
    
    
    private static Method getImplicitMethod(final Class< ? > clazz, final String methodName )
    {
        final Set< Method > candidates = new HashSet<>();
        if ( null != clazz.getSuperclass() )
        {
            candidates.add( getMethodInternal( clazz.getSuperclass(), methodName ) );
        }
        for ( final Class< ? > c : clazz.getInterfaces() )
        {
            candidates.add( getMethodInternal( c, methodName ) );
        }
        
        candidates.remove( null );
        if ( candidates.isEmpty() )
        {
            return null;
        }
        if ( 1 < candidates.size() )
        {
            throw new RuntimeException(
                    "Multiple candidates found for '" + methodName + "' on " + clazz + "'." );
        }
        
        return candidates.iterator().next();
    }
    
    
    private final static Map< Class< ? >, Map< Class< ? >, Map< Object, Object > > > ENUM_VALUE_CONV_MAP =
            new HashMap<>();
    private final static Map< Class< ? >, Class< ? > > PRIMITIVE_TO_NON_PRIMITIVE_MAP = new HashMap<>();
    private final static Map< Class< ? >, Class< ? > > NON_PRIMITIVE_TO_PRIMITIVE_MAP = new HashMap<>();
    private final static Method ENUM_VALUE_OF;
    
    static
    {
        try
        {
            ENUM_VALUE_OF = Enum.class.getMethod( "valueOf", Class.class, String.class );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to find valueOf method.", ex );
        }
        
        PRIMITIVE_TO_NON_PRIMITIVE_MAP.put( boolean.class, Boolean.class );
        PRIMITIVE_TO_NON_PRIMITIVE_MAP.put( byte.class, Byte.class );
        PRIMITIVE_TO_NON_PRIMITIVE_MAP.put( short.class, Short.class );
        PRIMITIVE_TO_NON_PRIMITIVE_MAP.put( int.class, Integer.class );
        PRIMITIVE_TO_NON_PRIMITIVE_MAP.put( long.class, Long.class );
        PRIMITIVE_TO_NON_PRIMITIVE_MAP.put( double.class, Double.class );
        PRIMITIVE_TO_NON_PRIMITIVE_MAP.put( float.class, Float.class );
        
        for ( final Map.Entry< Class< ? >, Class< ? > > e : PRIMITIVE_TO_NON_PRIMITIVE_MAP.entrySet() )
        {
            NON_PRIMITIVE_TO_PRIMITIVE_MAP.put( e.getValue(), e.getKey() );
        }
    }
}
