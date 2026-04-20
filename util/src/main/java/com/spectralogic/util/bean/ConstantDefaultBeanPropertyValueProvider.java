/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

final class ConstantDefaultBeanPropertyValueProvider implements DefaultBeanPropertyValueProvider
{
    ConstantDefaultBeanPropertyValueProvider( final Object constantValue )
    {
        m_constantValue = constantValue;
    }
    
    
    public Object getDefaultValue()
    {
        return m_constantValue;
    }
    
    
    private final Object m_constantValue;
}
