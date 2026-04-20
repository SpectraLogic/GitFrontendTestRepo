/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
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
 * Specifies that non annotated (ie runtime) columns used to sort by do not require an index to be valid.
 * WARNING: Use this annotation very carefully, as if a non-indexed field on a multi million entry table
 * is the target of the sort, it may take a substantial amount of time for the query to return.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public @interface SortByIndexNotNeeded
{
    // empty
}
