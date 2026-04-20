/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

/**
 * If your {@link com.spectralogic.util.db.lang.DatabasePersistable} implements this interface, you will
 * not be allowed to perform any modification operations (e.g. creates, modifies, or deletes) on that type.
 * <br><br>
 * Note: Beans with this annotation will not be validated for property nullness, default values, etc., since
 * if the bean is read only, that means that integrity guarantees must be performed elsewhere.
 */
public interface ReadOnly
{
    // empty
}
