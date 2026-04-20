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
 * Annotate a bean getter method with this annotation to provide it with a custom marshaled name.
 */
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.METHOD )
public @interface CustomMarshaledName
{
    /**
     * The marshaled name to use for the element (if the bean property type is an array or collection, this
     * is the element name that will be used to demark each element)
     */
    String value();
    
    
    /**
     * Only applies to bean properties of type array or collection; this is the element name that will be
     * used to demark the entire collection of elements (by default, no such demarkation is used).  If a
     * value is defined for this property, a value must also be defined for 
     * {@link #collectionValueRenderingMode}.
     */
    String collectionValue() default "";
    
    
    /**
     * Only applies to bean properties of type array or collection that define {@link #collectionValue} and
     * specifies how the collection value is to be rendered.
     */
    CollectionNameRenderingMode collectionValueRenderingMode() default CollectionNameRenderingMode.UNDEFINED;
    
    
    public enum CollectionNameRenderingMode
    {
        UNDEFINED,
        SINGLE_BLOCK_FOR_ALL_ELEMENTS,
        BLOCK_FOR_EVERY_ELEMENT
    }
}
