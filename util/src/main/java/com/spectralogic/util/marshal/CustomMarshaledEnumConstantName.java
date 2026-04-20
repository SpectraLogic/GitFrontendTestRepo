/*******************************************************************************
 *
 * Copyright C 2026, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate an enum constant with this annotation to provide it with a custom marshaled name.
 * This allows the Java enum constant identifier to be renamed without changing the value exposed
 * in the API, preserving backward compatibility.
 */
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.FIELD )
public @interface CustomMarshaledEnumConstantName
{
    /**
     * The name to use when marshaling this enum constant to XML or JSON and when documenting
     * it in the API contract.
     */
    String value();
}
