/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate a {@link com.spectralogic.util.db.lang.DatabasePersistable} bean with this annotation to
 * configure non-default log levels for operations performed on beans of that type.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public @interface ConfigureSqlLogLevels
{
    SqlLogLevels value() default SqlLogLevels.DEFAULT;
}
