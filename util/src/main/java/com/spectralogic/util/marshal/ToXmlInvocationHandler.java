/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import com.spectralogic.util.lang.NamingConventionType;

/**
 * An invocation handler that will respond to calls to get the XML representation of a bean.
 */
public final class ToXmlInvocationHandler implements InvocationHandler
{
    public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
    {
        if ( GET_XML_WITHOUT_NC_METHOD.equals( method ) )
        {
            return XmlMarshaler.marshal( proxy, BaseMarshalable.DEFAULT_NAMING_CONVENTION );
        }

        if ( GET_XML_WITH_NC_METHOD.equals( method ) )
        {
            return XmlMarshaler.marshal( proxy, (NamingConventionType)args[ 0 ] );
        }
        
        throw new UnsupportedOperationException( 
                "Invocation handler does not respond to " + method + "." );
    }
    
    
    private final static Method GET_XML_WITHOUT_NC_METHOD;
    private final static Method GET_XML_WITH_NC_METHOD;
    
    static
    {
        try
        {
            GET_XML_WITHOUT_NC_METHOD = Marshalable.class.getMethod( "toXml" );
            GET_XML_WITH_NC_METHOD = Marshalable.class.getMethod( "toXml", NamingConventionType.class );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
}
