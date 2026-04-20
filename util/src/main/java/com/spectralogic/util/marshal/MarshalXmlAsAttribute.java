/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * By default, bean properties are marshaled as DOM elements and not as attributes of their parent.  With this
 * annotation on a bean property's reader, the property will be marshaled as an attribute of the parent type.
 * <br><br>
 * 
 * For example,
 * <human>
 *   <height>22</height>
 * </human>
 * 
 * would be the default marshaling, but with this annotation on the height property, the marshaling would
 * change to: <br><br>
 * 
 * <human height=22/>
 */
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.METHOD )
public @interface MarshalXmlAsAttribute
{
    // empty
}
