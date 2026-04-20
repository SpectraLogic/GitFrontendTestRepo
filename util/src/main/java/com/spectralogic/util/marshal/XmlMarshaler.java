/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.MarshalableElement.ElementRenderingMode;

public final class XmlMarshaler
{
    private XmlMarshaler()
    {
        // singleton
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
        
        Class< ? > clazz = BeanFactory.getType( value.getClass() );
        if ( Marshalable.class.isAssignableFrom( clazz ) )
        {
            return generateXml(
                    null, 
                    value,
                    namingConvention,
                    useObjectMarshaler );
        }
        if ( clazz.isArray() )
        {
            value = CollectionFactory.toList( ReflectUtil.toObjectArray( value ) );
            clazz = value.getClass();
        }
        if ( Collection.class.isAssignableFrom( clazz ) )
        {
            final StringBuilder sb = new StringBuilder( 800 );
            for ( final Object o : (Collection<?>)value )
            {
                final Class< ? > beanType = BeanFactory.getType( o.getClass() );
                String tagName = beanType.getSimpleName();
                final CustomMarshaledTypeName customTagName =
                        beanType.getAnnotation( CustomMarshaledTypeName.class );
                if ( null != customTagName )
                {
                    tagName = customTagName.value();
                }
                sb.append( getStartTag( tagName, namingConvention ) + generateXml( 
                    null, 
                    o,
                    namingConvention,
                    true ) + getEndTag( tagName, namingConvention ) );
            }
            return sb.toString();
        }
        throw new UnsupportedOperationException( "No code to handle " + clazz + "." );
    }
    
    
    private static String generateXml( 
            final MarshalableElement marshalableElement,
            Object value,
            final NamingConventionType namingConvention,
            final boolean useObjectMarshaler )
    {
        final String elementName = ( null == marshalableElement ) ? 
                null : marshalableElement.getName();
        final String collectionName = ( null == marshalableElement ) ? 
                null : marshalableElement.getCollectionName();
        final CollectionNameRenderingMode collectionRenderingMode = ( null == marshalableElement ) ?
                null : marshalableElement.getCollectionNameRenderingMode();
        
        if ( null == value )
        {
            return getFullTag( elementName, namingConvention );
        }
        
        final Class< ? > clazz = value.getClass();
        if ( clazz.isArray() || Collection.class.isAssignableFrom( clazz ) )
        {
            if ( clazz.isArray() )
            {
                value = CollectionFactory.toList( (Object[])value );
            }
            
            final StringBuilder retval = new StringBuilder( 400 );
            if ( ( (Collection< ? >)value ).isEmpty() && null != collectionName 
                    && CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS == collectionRenderingMode )
            {
                retval.append( getFullTag( collectionName, namingConvention ) );
            }
            else
            {
                if ( CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS == collectionRenderingMode )
                {
                    retval.append( getStartTag( collectionName, namingConvention ) );
                }
                for ( final Object o : (Iterable< ? >)value )
                {
                    if ( CollectionNameRenderingMode.BLOCK_FOR_EVERY_ELEMENT == collectionRenderingMode )
                    {
                        final List< MarshalableElement > attributes = 
                                getAttributes( MarshalUtil.getMarshalableElements( o ) );
                        retval.append( getStartTag(
                                collectionName, 
                                buildAttributesText( attributes, namingConvention ),
                                namingConvention ) );
                    }
                    retval.append( generateXml( marshalableElement, o, namingConvention, true ) );
                    if ( CollectionNameRenderingMode.BLOCK_FOR_EVERY_ELEMENT == collectionRenderingMode )
                    {
                        retval.append( getEndTag( collectionName, namingConvention ) );
                    }
                }
                if ( CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS == collectionRenderingMode )
                {
                    retval.append( getEndTag( collectionName, namingConvention ) );
                }
            }
            
            return retval.toString();
        }
        
        if ( Marshalable.class.isAssignableFrom( clazz ) )
        {
            return generateXmlForBean( elementName, value, namingConvention, useObjectMarshaler );
        }
        
        return getStartTag( elementName, namingConvention ) 
                + buildSafeValueText( value ) 
                + getEndTag( elementName, namingConvention );
    }
    

    private static String generateXmlForBean( 
            final String topLevelXmlElementName,
            final Object bean,
            final NamingConventionType namingConvention,
            final boolean useObjectMarshaler )
    {
        final List< MarshalableElement > elements = MarshalUtil.getMarshalableElements( bean );
        final List< MarshalableElement > attributes = getAttributes( elements );
        if ( attributes.isEmpty() && useObjectMarshaler )
        {
            return getStartTag( topLevelXmlElementName, namingConvention ) 
                   + ((Marshalable)bean).toXml( namingConvention ) 
                   + getEndTag( topLevelXmlElementName, namingConvention );
        }

        final int numberOfChildren = getNumberOfChildren( elements );
        final StringBuilder retval = new StringBuilder( 400 );
        
        if ( 0 < numberOfChildren )
        {
            retval.append( getStartTag(
                    topLevelXmlElementName, 
                    buildAttributesText( attributes, namingConvention ), 
                    namingConvention ) );
            for ( final MarshalableElement e : elements )
            {
                if ( ElementRenderingMode.CHILD == e.getElementRenderingMode() )
                {
                    retval.append( generateXml( e, e.getValue(), namingConvention, true ) );
                }
            }
            retval.append( getEndTag( topLevelXmlElementName, namingConvention ) );
        }
        else
        {
            retval.append( getFullTag(
                    topLevelXmlElementName,
                    buildAttributesText( attributes, namingConvention ),
                    namingConvention ) );
        }
        return retval.toString();
    }
    
    
    public static List< MarshalableElement > getAttributes( final List< MarshalableElement > elements )
    {
        final List< MarshalableElement > retval = new ArrayList<>();
        for ( final MarshalableElement e : elements )
        {
            if ( ElementRenderingMode.ATTRIBUTE == e.getElementRenderingMode() )
            {
                retval.add( e );
            }
        }
        return retval;
    }
    
    
    private static int getNumberOfChildren( final List< MarshalableElement > elements )
    {
        int retval = 0;
        for ( final MarshalableElement e : elements )
        {
            if ( ElementRenderingMode.CHILD == e.getElementRenderingMode() )
            {
                retval += 1;
            }
        }
        return retval;
    }
    
    
    public static String buildAttributesText( 
            final List< MarshalableElement > attributes,
            final NamingConventionType namingConvention )
    {
        final StringBuilder retval = new StringBuilder();
        for ( final MarshalableElement e : attributes )
        {
            final Object value = e.getValue();
            if ( null != value )
            {
                retval
                        .append( " " )
                        .append( namingConvention.convert( e.getName() ) )
                        .append( "=\"" )
                        .append( buildSafeValueText( value ) )
                        .append( "\"" );
            }
        }
        return retval.toString();
    }
    
    
    private static String buildSafeValueText( Object value )
    {
        if ( value instanceof Date )
        {
            value = DateMarshaler.marshal( (Date)value );
        }
        if ( value.getClass().isEnum() )
        {
            return StringEscapeUtils.escapeXml10( MarshalUtil.getMarshaledEnumName( value ) );
        }
        return StringEscapeUtils.escapeXml10( value.toString() ) ;
    }
    
    
    private static String getStartTag( final String name, final NamingConventionType namingConvention )
    {
        return getStartTag( name, "", namingConvention );
    }
    
    
    private static String getStartTag(
            final String name, 
            final String attributesText,
            final NamingConventionType namingConvention )
    {
        if ( null == name || name.isEmpty() )
        {
            return "";
        }
        return "<" + namingConvention.convert( name ) + attributesText + ">";
    }
    
    
    private static String getEndTag( final String name, final NamingConventionType namingConvention )
    {
        if ( null == name || name.isEmpty() )
        {
            return "";
        }
        return "</" + namingConvention.convert( name ) + ">";
    }
    
    
    private static String getFullTag( final String name, final NamingConventionType namingConvention )
    {
        return getFullTag( name, "", namingConvention );
    }
    
    
    private static String getFullTag( 
            final String name,
            final String attributesText,
            final NamingConventionType namingConvention )
    {
        if ( null == name || name.isEmpty() )
        {
            return "";
        }
        
        final StringBuilder retval = new StringBuilder();
        retval.append( "<" + namingConvention.convert( name ) );
        retval.append( attributesText );
        retval.append( "/>" );
        return retval.toString();
    }
    
    
    public static String formatPretty( final String sourceXml ) 
    {
        Validations.verifyNotNull( "Source XML", sourceXml );
        try 
        {
            final Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
            transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "yes" );
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
 
            final Source xmlSource = new SAXSource( new InputSource( new ByteArrayInputStream( 
                    sourceXml.getBytes() ) ) );
            final StreamResult res = new StreamResult( new ByteArrayOutputStream() );
 
            transformer.transform( xmlSource, res );
 
            return new String( ( (ByteArrayOutputStream)res.getOutputStream() ).toByteArray() );
        } 
        catch ( final Exception ex ) 
        {
            LOG.warn( "Failed to transform XML to pretty format: " 
                      + LogUtil.getShortVersion( sourceXml ), ex );
            return sourceXml;
        }
    }
    
    
    private final static Logger LOG = Logger.getLogger( XmlMarshaler.class );
}
