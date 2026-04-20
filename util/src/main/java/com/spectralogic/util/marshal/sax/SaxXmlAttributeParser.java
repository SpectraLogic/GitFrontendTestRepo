/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal.sax;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;

public final class SaxXmlAttributeParser
{
    public enum AttributeIs
    {
        REQUIRED,
        OPTIONAL
    }
    
    
    public SaxXmlAttributeParser( final String elementName, final Attributes attributes )
    {
        Validations.verifyNotNull( "Element name", elementName );
        Validations.verifyNotNull( "Attributes", attributes );
        
        m_elementName = elementName;
        m_attributes = parseAttributes( attributes );
    }
    
    
    public void logAttributesParsed()
    {
        m_logWorthy = true;
    }

    
    private static Map< String, String > parseAttributes( final Attributes attributes )
    {
        final Map< String, String > retval = new HashMap<>();
        for ( int i = 0; i < attributes.getLength(); ++i )
        {
            retval.put( 
                    NamingConventionType.CONCAT_LOWERCASE.convert( attributes.getQName( i ) ),
                    attributes.getValue( i ) );
        }
        return retval;
    }
    
    
    public String getString( final String attributeName, final AttributeIs attributeSpecification )
    {
        final String retval = getValue( attributeName );
        validate( attributeName, retval, attributeSpecification );
        return retval;
    }
    
    
    public UUID getUUID( final String attributeName, final AttributeIs attributeSpecification )
    {
        final String retval = getValue( attributeName );
        validate( attributeName, retval, attributeSpecification );
        if ( null == retval )
        {
            return null;
        }
        try
        {
            return UUID.fromString( retval );
        }
        catch ( final NumberFormatException ex )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.BAD_REQUEST, 
                    attributeName + " is invalid (could not parse as UUID): " + retval, ex );
        }
    }
    
    
    public Long getLong( final String attributeName, final AttributeIs attributeSpecification )
    {
        final Long retval = getLongInternal( attributeName, getValue( attributeName ) );
        validate( attributeName, retval, attributeSpecification );
        return retval;
    }
    
    
    private Long getLongInternal( final String attributeName, final String value )
    {
        if ( null == value )
        {
            return null;
        }
        
        try
        {
            return Long.valueOf( value );
        }
        catch ( final NumberFormatException ex )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.BAD_REQUEST, 
                    attributeName + " is invalid (could not parse as long): " + value, ex );
        }
    }
    
    
    public < T > T getEnumConstant( 
            final Class< T > clazz,
            final String attributeName, 
            final AttributeIs attributeSpecification )
    {
        final T retval = getEnumConstantInternal( clazz, getValue( attributeName ) );
        validate( attributeName, retval, attributeSpecification );
        return retval;
    }
    
    
    private String getValue( final String attributeName )
    {
        return m_attributes.get( NamingConventionType.CONCAT_LOWERCASE.convert( attributeName ) );
    }
    
    
    private < T > T getEnumConstantInternal( final Class< T > clazz, final String value )
    {
        if ( null == value )
        {
            return null;
        }
        
        try
        {
            return ReflectUtil.enumValueOf( clazz, value );
        }
        catch ( final RuntimeException ex )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.BAD_REQUEST, 
                    "'" + value + "' is not a valid value for " + clazz.getSimpleName() 
                    + ".  Allowed values are: " + CollectionFactory.toList( clazz.getEnumConstants() ), 
                    ex );
        }
    }
    
    
    public void verifyAllAttributesHaveBeenConsumed()
    {
        final Map< String, String > attributes = new HashMap<>( m_attributes );
        for ( final String gottenAttribute : m_gottenAttributes )
        {
            attributes.remove( gottenAttribute );
        }
        
        attributes.remove( "xmlns" ); // ignore the namespace if one is provided
        if ( !attributes.isEmpty() )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST, 
                    "Invalid XML payload sent by client: Unexpected attributes found for DOM element " 
                    + m_elementName + ": " + attributes );
        }
    }
    
    
    private void validate( 
            final String attributeName,
            final Object retval, 
            final AttributeIs specification )
    {
        m_gottenAttributes.add( NamingConventionType.CONCAT_LOWERCASE.convert( attributeName ) );
        if ( AttributeIs.REQUIRED == specification && null == retval )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.BAD_REQUEST,
                    "Invalid XML payload sent by client: Expected attribute '" + attributeName 
                    + "' on DOM element '" + m_elementName + "', but attribute was not present." );
        }
        if ( m_logWorthy )
        {
            LOG.info( "For DOM element " + m_elementName + ", parsed attribute " + attributeName + ": " 
                      + retval );
        }
    }
    
    
    private volatile boolean m_logWorthy;
    
    private final String m_elementName;
    private final Map< String, String > m_attributes;
    private final Set< String > m_gottenAttributes = new HashSet<>();
    
    private final static Logger LOG = Logger.getLogger( SaxXmlAttributeParser.class );
} // end inner class def