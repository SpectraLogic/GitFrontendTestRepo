/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.lang.NamingConventionType;

public class BaseMarshalable implements Marshalable
{
    @Override
    public String toJson( final NamingConventionType namingConvention )
    {
        return JsonMarshaler.marshal( this, namingConvention );
    }
    
    
    @Override
    public String toJson()
    {
        return toJson( DEFAULT_NAMING_CONVENTION );
    }

    
    @Override
    public String toXml( final NamingConventionType namingConvention )
    {
        return XmlMarshaler.marshal( this, namingConvention );
    }

    
    @Override
    public String toXml()
    {
        return toXml( DEFAULT_NAMING_CONVENTION );
    }
    
    
    @Override
    public String toString()
    {
        final Map< String, Object > sanitizedBeanPropertyValues = new HashMap<>();
        for ( final String prop : BeanUtils.getPropertyNames( getClass() ) )
        {
            final Method reader = BeanUtils.getReader( getClass(), prop );
            if ( null == reader )
            {
                continue;
            }
            if ( null == reader.getAnnotation( Secret.class ) )
            {
                try
                {
                    sanitizedBeanPropertyValues.put( prop, reader.invoke( this ) );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
        return getClass().getName() + "@" + sanitizedBeanPropertyValues;
    }
    

    public final static NamingConventionType DEFAULT_NAMING_CONVENTION =
            NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE;
}
