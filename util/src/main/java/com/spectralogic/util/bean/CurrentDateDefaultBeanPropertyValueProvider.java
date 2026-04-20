/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.util.Date;

final class CurrentDateDefaultBeanPropertyValueProvider implements DefaultBeanPropertyValueProvider
{
    public Object getDefaultValue()
    {
        return new Date();
    }
}
