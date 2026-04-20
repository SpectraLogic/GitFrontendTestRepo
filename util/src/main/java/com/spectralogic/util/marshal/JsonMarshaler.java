package com.spectralogic.util.marshal;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.spectralogic.util.bean.lang.Optional;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.log.LogUtil;

public class JsonMarshaler 
{
    private JsonMarshaler()
    {
        // do not instantiate me
    }
    
    
    public static String marshal( Object value, final NamingConventionType namingConvention )
    {
        return marshal( value, namingConvention, false );
    }
    
    
    private static String marshal( 
            Object value, 
            final NamingConventionType namingConvention,
            final boolean useObjectMarshaler )
    {
        if ( null == value )
        {
            return "null";
        }
        
        final Class< ? > clazz = value.getClass();
        if ( clazz.isArray() || Collection.class.isAssignableFrom( clazz ) )
        {
            if ( clazz.isArray() )
            {
                value = CollectionFactory.toList( ReflectUtil.toObjectArray( value ) );
            }
            
            final StringBuilder retval = new StringBuilder( 400 );
            retval.append( "[" );
            boolean needsComma = false;
            for ( final Object o : (Iterable< ? >)value )
            {
                if ( needsComma )
                {
                    retval.append( "," );
                }
                needsComma = true;
                retval.append( marshal( o, namingConvention, true ) );
            }
            retval.append( "]" );
            return retval.toString();
        }
        
        if ( Marshalable.class.isAssignableFrom( clazz ) )
        {
            if ( useObjectMarshaler )
            {
                return ((Marshalable)value).toJson( namingConvention );
            }
            return generateJsonForBean( value, namingConvention );
        }
        final Class< ? > nonPrimitiveValueType = ReflectUtil.toNonPrimitiveType( value.getClass() );
        if ( Date.class.isAssignableFrom( nonPrimitiveValueType ) )
        {
            return String.valueOf( ( (Date)value ).getTime() );
        }
        else if ( Number.class.isAssignableFrom( nonPrimitiveValueType )
                || Boolean.class.isAssignableFrom( nonPrimitiveValueType ) )
        {
            return value.toString();
        }
        return "\"" + jsonEncode( MarshalUtil.getStringFromTypedValue( value ) ) + "\"";
    }
    
    
    private static String generateJsonForBean( 
            final Object bean,
            final NamingConventionType namingConvention )
    {
        final StringBuilder retval = new StringBuilder( 800 );
        retval.append("{");
        
        boolean needsComma = false;
        for ( final MarshalableElement e : MarshalUtil.getMarshalableElements( bean ) )
        {
            if ( needsComma )
            {
                retval.append( "," );
            }
            
            needsComma = true;
            final String name = ( null == e.getCollectionName() ) ? e.getName() : e.getCollectionName();
            retval.append( "\"" + namingConvention.convert( name ) + "\":" );
            retval.append( marshal( e.getValue(), namingConvention, true ) );
        }
        retval.append( "}" );
        return retval.toString();
    }
    

    public static String jsonEncode( final String str )
    {
        if (str == null)
        {
            return null;
        }

        final StringBuilder retval = new StringBuilder( str );
        replace( retval, "\\", "\\\\" );
        for ( final Map.Entry< String, String > entry : JSON_REPLACEMENTS.entrySet() )
        {
            replace( retval, entry.getKey(), entry.getValue() );
        }
        return retval.toString();
    }
    
    
    private static void replace(
            final StringBuilder sb, 
            final String stringToReplace,
            final String replacement )
    {
        int index = 0;
        while ( true )
        {
            index = sb.indexOf( stringToReplace, index );
            if ( 0 > index )
            {
                return;
            }
            sb.replace( index, index + stringToReplace.length(), replacement );
            index += replacement.length();
        }
    }
    
    
    public static String formatPretty( final String json )
    {
        final Duration duration = new Duration();
        try
        {
            return new JSONObject( json ).toString( 2 );
        }
        catch ( final JSONException ex )
        {
            try
            {
                return new JSONArray( json ).toString( 2 );
            }
            catch ( final JSONException ex2 )
            {
                LOG.error( "Failed to format json into pretty format as a JSON Object: " 
                           + LogUtil.getShortVersion( json ), ex );
                LOG.error( "Failed to format json into pretty format as a JSON array: "
                           + LogUtil.getShortVersion( json ), ex2 );
                return json;
            }
        }
        finally
        {
            if ( 9 < duration.getElapsedSeconds() )
            {
                LOG.warn( "It took " + duration + " to format JSON: " + LogUtil.getShortVersion( json ) );
            }
            else if ( 3 < duration.getElapsedSeconds() )
            {
                LOG.info( "It took " + duration + " to format JSON: " + LogUtil.getShortVersion( json ) );
            }
        }
    }
    
    
    public static < T extends SimpleBeanSafeToProxy > T unmarshal( 
            final Class< T > typeToUnmarshalTo, 
            final String jsonRepresentation )
    {
        try
        {
            @SuppressWarnings( "unchecked" )
            final T retval = (T)unmarshalJson(
                    typeToUnmarshalTo, new JSONObject( jsonRepresentation ) );
            return retval;
        }
        catch ( final JSONException ex )
        {
            throw new RuntimeException(
                    "Json representation was invalid: " + LogUtil.getShortVersion( jsonRepresentation ), ex );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException(
                    "Json could not be unmarshaled: " + LogUtil.getShortVersion( jsonRepresentation ), ex );
        }
    }
    
    
    private static Object unmarshalJson( 
            final Class< ? > typeToUnmarshalTo,
            final JSONObject json )
    {
        if ( SimpleBeanSafeToProxy.class.isAssignableFrom( typeToUnmarshalTo ) )
        {
            final Map< String, String > jsonProperties = new HashMap<>();
            for ( final String jsonProperty : CollectionFactory.toList( JSONObject.getNames( json ) ) )
            {
                jsonProperties.put(
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( jsonProperty ), 
                        jsonProperty );
            }
            
            @SuppressWarnings( "unchecked" )
            final SimpleBeanSafeToProxy retval = 
                    BeanFactory.newBean( (Class< SimpleBeanSafeToProxy >)typeToUnmarshalTo );
            for ( final String prop : BeanUtils.getPropertyNames( typeToUnmarshalTo ) )
            {
                String attributeName = prop;
                if ( !jsonProperties.containsKey( prop ) )
                {
                    final Method reader = BeanUtils.getReader( typeToUnmarshalTo, prop );
                    final CustomMarshaledName customName = reader.getAnnotation( CustomMarshaledName.class );
                    if ( null != customName && null != customName.collectionValue()
                            && jsonProperties.containsKey( 
                                    NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert(
                                            customName.collectionValue() ) ) )
                    {
                        attributeName = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( 
                                customName.collectionValue() );
                    }
                    else if ( null != customName && null != customName.value()
                            && jsonProperties.containsKey( 
                                    NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert(
                                            customName.value() ) ) )
                    {
                        attributeName = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( 
                                customName.value() );
                    }
                    else if ( reader.getReturnType().isPrimitive() )
                    {
                        throw new RuntimeException(
                                "JSON must specify value for "
                                + typeToUnmarshalTo.getName() + "." + prop + "." );
                    }
                    else
                    {
                        continue;
                    }
                }
                
                Object value = "{failed to determine}";
                try
                {
                    value = unmarshalBeanProperty( 
                            typeToUnmarshalTo,
                            prop,
                            json, 
                            jsonProperties.get( attributeName ) );
                    final Method writer = BeanUtils.getWriter( typeToUnmarshalTo, prop );
                    writer.setAccessible( true );
                    try
                    {
                        writer.invoke( retval, new Object [] { value } );
                    }
                    catch ( final Exception ex )
                    {
                        throw new RuntimeException( 
                                "Failed to invoke " + writer + " on " + retval 
                                + " with bean property value " 
                                + ( ( null == value ) ? "" : " of type " + value.getClass().getSimpleName() ) 
                                + ": " + value, ex );
                    }
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( 
                            "Failed to unmarshal to " + typeToUnmarshalTo.getName() + "." + prop + ".", ex );
                }
            }
            return retval;
        }

        throw new UnsupportedOperationException( 
                "No code to support unmarshaling a JSON object that isn't a " 
                + SimpleBeanSafeToProxy.class.getSimpleName() + "." );
    }
    
    
    private static Object unmarshalJson( final Class< ? > typeToUnmarshalTo, final String jsonValue )
    {
        try
        {
            return MarshalUtil.getTypedValueFromNullableString(
                    typeToUnmarshalTo, 
                    jsonValue );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( 
                    "Failed to unmarshal '" + jsonValue + "' to " + typeToUnmarshalTo + ".", ex );
        }
    }
    
    
    private static Object unmarshalBeanProperty(
            final Class< ? > beanType, 
            final String prop, 
            final JSONObject json,
            final String jsonProp )
    {
        final Method reader = BeanUtils.getReader( beanType, prop );
        final Class< ? > propType = ReflectUtil.toNonPrimitiveType( reader.getReturnType() );
        
        if ( Collection.class.isAssignableFrom( propType ) )
        {
            throw new UnsupportedOperationException( 
                    Collection.class.getSimpleName() 
                    + " types are not supported, "
                    + "since their component type information is erased at runtime.  " 
                    + "You must use an array instead.  Reader method in violation: " 
                    + reader.getName() );
        }
        
        if ( propType.isArray() )
        {
            final JSONArray jsonValue;
            try
            {
                jsonValue = json.getJSONArray( jsonProp );
            }
            catch ( final JSONException ex )
            {
                throw new RuntimeException( "Failed to get JSON value for " + prop + ".", ex );
            }
            
            final Object retval = Array.newInstance( 
                    propType.getComponentType(),
                    jsonValue.length() );
            for ( int i = 0; i < jsonValue.length(); ++i )
            {
                try
                {
                    if ( jsonValue.isNull( i ) )
                    {
                        Array.set( retval, i, null );
                    }
                    else if ( SimpleBeanSafeToProxy.class.isAssignableFrom( propType.getComponentType() ) )
                    {
                        Array.set( retval, i, unmarshalJson( 
                                propType.getComponentType(), jsonValue.getJSONObject( i ) ) );
                    }
                    else if ( Number.class.isAssignableFrom(
                            ReflectUtil.toNonPrimitiveType( propType.getComponentType() ) ) )
                    {
                        Array.set( retval, i, Long.valueOf( jsonValue.getLong( i ) ) );
                    }
                    else
                    {
                        Array.set( retval, i, unmarshalJson( 
                                propType.getComponentType(), jsonValue.getString( i ) ) );
                    }
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException(
                            "Failed to marshal JSON array value at element " + i + ".", ex );
                }
            }
            
            return retval;
        }

        try
        {
            return unmarshalJson( propType, json.getString( jsonProp ) );
        }
        catch ( final JSONException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    

    private final static Logger LOG = Logger.getLogger( JsonMarshaler.class );
    private final static Map< String, String > JSON_REPLACEMENTS = new HashMap<>();
    static
    {
        JSON_REPLACEMENTS.put( Platform.SLASH_N, "\\n" );
        JSON_REPLACEMENTS.put( "\"", "\\\"" );
        JSON_REPLACEMENTS.put( "/", "\\/" );
        JSON_REPLACEMENTS.put( "\b", "\\b" );
        JSON_REPLACEMENTS.put( "\f", "\\f" );
        JSON_REPLACEMENTS.put( Platform.SLASH_R, "\\r" );
        JSON_REPLACEMENTS.put( "\t", "\\t" );
    }
}
