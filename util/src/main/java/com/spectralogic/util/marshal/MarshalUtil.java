/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalableElement.ElementRenderingMode;

public final class MarshalUtil
{
    private MarshalUtil()
    {
        // singleton
    }
    
    
    public static < B > B newBean( final Class< B > clazz, final Map< String, String > initialPropValues )
    {
        final Map< String, Object > typedInitialPropValues = new HashMap<>();
        for ( final String prop : BeanUtils.getPropertyNames( clazz ) )
        {
            if ( initialPropValues.containsKey( prop ) || initialPropValues.containsKey( prop.toLowerCase() ) )
            {
                String value = initialPropValues.get( prop ) != null ?
                        initialPropValues.get( prop ) : initialPropValues.get( prop.toLowerCase() );
                typedInitialPropValues.put( 
                        prop,
                        getTypedValueFromNullableString( 
                                BeanUtils.getReader( clazz, prop ).getReturnType(),
                                value ) );
            }
        }
        
        return BeanFactory.newBean( clazz, typedInitialPropValues );
    }
    
    
    public static String getStringFromTypedValue( final Object value )
    {
        if ( null == value )
        {
            return null;
        }

        if ( Date.class.isAssignableFrom( value.getClass() ) )
        {
            return String.valueOf( ( (Date)value ).getTime() );
        }
        if ( Marshalable.class.isAssignableFrom( value.getClass() ) )
        {
            return JsonMarshaler.formatPretty(
                    ( (Marshalable)value ).toJson( NamingConventionType.UNDERSCORED ) );
        }
        if ( value.getClass().isEnum() )
        {
            return getMarshaledEnumName( value );
        }

        return value.toString();
    }


    /**
     * Returns the marshaled name for an enum constant.  If the constant is annotated with
     * {@link CustomMarshaledEnumConstantName}, that value is returned; otherwise the constant's
     * {@code toString()} result is returned.
     */
    public static String getMarshaledEnumName( final Object enumValue )
    {
        try
        {
            final Field field = enumValue.getClass().getField( ( (Enum< ? >)enumValue ).name() );
            final CustomMarshaledEnumConstantName annotation =
                    field.getAnnotation( CustomMarshaledEnumConstantName.class );
            if ( null != annotation )
            {
                return annotation.value();
            }
        }
        catch ( final NoSuchFieldException ex )
        {
            // fall through to toString()
        }
        return enumValue.toString();
    }
    
    
    public static Object getTypedValueFromNullableString( Class< ? > type, final String value )
    {
        if ( "null".equals( value ) )
        {
            return null;
        }
        return getTypedValueFromString( type, value );
    }
    
    
    public static Object getTypedValueFromString( Class< ? > type, final String value )
    {
        if ( null == value )
        {
            return null;
        }
        
        type = ReflectUtil.toNonPrimitiveType( type );
        if ( String.class == type )
        {
            return value;
        }
        if ( Integer.class == type )
        {
            return Integer.valueOf( value );
        }
        if ( Double.class == type )
        {
            return Double.valueOf( value );
        }
        if ( Long.class == type )
        {
            return Long.valueOf( value );
        }
        if ( Boolean.class == type )
        {
            return Boolean.valueOf( value );
        }
        if ( UUID.class == type )
        {
            return UUID.fromString( value );
        }
        if ( Date.class == type )
        {
            try
            {
                return new Date( Long.valueOf( value ).longValue() );
            }
            catch ( final NumberFormatException ex )
            {
                Validations.verifyNotNull( "It may not be a long.", ex );
                return DateMarshaler.unmarshal( value );
            }
        }
        if ( type.isEnum() )
        {
            for ( final Object constant : type.getEnumConstants() )
            {
                if ( value.equals( getMarshaledEnumName( constant ) ) )
                {
                    return constant;
                }
            }
            return ReflectUtil.enumValueOf( type, value );
        }
        if ( SimpleBeanSafeToProxy.class.isAssignableFrom( type ) )
        {
            @SuppressWarnings( "unchecked" )
            final Class< SimpleBeanSafeToProxy > castedType = (Class< SimpleBeanSafeToProxy >)type;
            return JsonMarshaler.unmarshal( castedType, value );
        }
        if ( void.class == type )
        {
            throw new IllegalArgumentException( 
                    "The expected type was void, so value is invalid: " + value );
        }
        
        throw new UnsupportedOperationException( 
                "No code to handle " + type + ", so cannot get typed value from " + value + " of type " 
                + value.getClass().getName() + "." );
    }
    
    
    public static List< MarshalableElement > getMarshalableElements( final Object bean )
    {
        final List< MarshalableElement > retval = new ArrayList<>();
        
        final Class< ? > type = BeanFactory.getType( bean.getClass() );
        final boolean excludeDefaultValues = type.getAnnotation( ExcludeDefaultsFromMarshaler.class ) != null;
        for ( final Map.Entry< MarshalableElement, Method > e : CACHE.get( type ).entrySet() )
        {
            final Method reader = e.getValue();
            final Class< ? > returnType = reader.getReturnType();
            
            final ExcludeFromMarshaler excludeFromMarshaler = 
                    reader.getAnnotation( ExcludeFromMarshaler.class );
            final When exclusionMode = 
                    ( null == excludeFromMarshaler ) ? null : excludeFromMarshaler.value();
            
            try
            {
                Object value = reader.invoke( bean );
                if ( null == value )
                {
                    if ( returnType.isArray() || Collection.class.isAssignableFrom( returnType ) )
                    {
                        value = new HashSet<>();
                    }
                }

                if (excludeDefaultValues && Objects.equals(BeanUtils.getDefaultBeanPropValue( type, e.getKey().getName() ), value)) {
                    continue;
                }
                if ( When.VALUE_IS_NULL == exclusionMode )
                {
                    if ( null == value )
                    {
                        continue;
                    }
                    if ( returnType.isArray() )
                    {
                        if ( value.getClass().isArray() && 0 == ( (Object[])value ).length )
                        {
                            continue;
                        }
                    }
                    if ( Collection.class.isAssignableFrom( returnType )
                            && ( (Collection<?>)value ).isEmpty() )
                    {
                        continue;
                    }
                }
                
                retval.add( new MarshalableElement(
                        e.getKey().getName(), 
                        e.getKey().getCollectionName(),
                        e.getKey().getCollectionNameRenderingMode(),
                        e.getKey().getElementRenderingMode(),
                        value ) );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to invoke reader: " + reader, ex );
            }
        }
        
        Collections.sort( retval );
        
        return retval;
    }
    
    
    private final static class MarshalableElementsCacheResultProvider 
        implements CacheResultProvider< Class< ? >, Map< MarshalableElement, Method > >
    {
        public Map< MarshalableElement, Method > generateCacheResultFor( Class< ? > type )
        {
            type = BeanFactory.getType( type );
            final Map< MarshalableElement, Method > retval = new HashMap<>();

            for ( final String prop : BeanUtils.getPropertyNames( type ) )
            {
                final Method reader = BeanUtils.getReader( type, prop );
                if ( null == reader )
                {
                    continue;
                }
                
                final ExcludeFromMarshaler excludeFromMarshaler = 
                        reader.getAnnotation( ExcludeFromMarshaler.class );
                final When exclusionMode = 
                        ( null == excludeFromMarshaler ) ? null : excludeFromMarshaler.value();
                if ( When.ALWAYS == exclusionMode )
                {
                    continue;
                }
                
                reader.setAccessible( true );

                final String collectionName;
                final CustomMarshaledName cmn = reader.getAnnotation( CustomMarshaledName.class );
                if ( null != cmn )
                {
                    collectionName = ( cmn.collectionValue().isEmpty() ) ?
                            null 
                            : NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( 
                                    cmn.collectionValue() );
                }
                else
                {
                    collectionName = null;
                }

                final CollectionNameRenderingMode collectionNameRenderingMode;
                if ( null != cmn )
                {
                    collectionNameRenderingMode = cmn.collectionValueRenderingMode();
                }
                else
                {
                    collectionNameRenderingMode = CollectionNameRenderingMode.UNDEFINED;
                }
                
                final String elementName;
                if ( null == reader.getAnnotation( CustomMarshaledName.class ) )
                {
                    elementName = prop;
                }
                else
                {
                    elementName = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert(
                            reader.getAnnotation( CustomMarshaledName.class ).value() );
                }
                
                final ElementRenderingMode elementRenderingMode = 
                        ( null == reader.getAnnotation( MarshalXmlAsAttribute.class ) ) ? 
                                ElementRenderingMode.CHILD
                                : ElementRenderingMode.ATTRIBUTE;
                try
                {
                    retval.put( new MarshalableElement(
                            elementName, 
                            collectionName, 
                            collectionNameRenderingMode,
                            elementRenderingMode,
                            null ), reader );
                }
                catch ( final RuntimeException ex )
                {
                    throw new RuntimeException(
                            "Failed to generate " + MarshalableElement.class.getSimpleName() + " for "
                            + type + "." + prop + ".", ex );
                }
            }
            
            return retval;
        }
    } // end inner class def
    
    
    private final static StaticCache< Class< ? >, Map< MarshalableElement, Method > > CACHE =
            new StaticCache<>( new MarshalableElementsCacheResultProvider() );
}
