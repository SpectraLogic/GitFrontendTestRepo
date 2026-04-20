/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate bean getter methods with this when you desire not to have that property included 
 * in any marshaler output (e.g. XML or JSON payloads) under certain circumstances.
 */
@Target( ElementType.METHOD )
@Retention( RetentionPolicy.RUNTIME )
public @interface ExcludeFromMarshaler
{
    public enum When
    {
        /**
         * Only exclude this bean property from the marshaled payload when the value is null or empty.
         */
        VALUE_IS_NULL,
        
        /**
         * Always exclude this bean property from the marshaled payload.
         */
        ALWAYS
    }
    
    
    When value();
}
