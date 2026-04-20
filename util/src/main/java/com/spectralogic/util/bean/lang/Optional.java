/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean.lang;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate bean getter methods that can be null in persistence, request/response payloads, etc. with this 
 * annotation.  Various infrastructure components may use this annotation to validate that required fields
 * are populated.  Properties are required by default and require this annotation to be made optional.  
 * <br><br>
 * 
 * Note: When this annotation is used on an array or collection, it means the array or collection may be
 * null or zero-length.  When this annotation is not used on the same, it means the array or collection may
 * not be null or zero-length.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.METHOD )
public @interface Optional
{
    // empty
}
