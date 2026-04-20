/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.List;

import com.spectralogic.util.db.lang.DatabasePersistable;

public interface WhereClause
{
    /**
     * @param sqlParameters - parameters corresponding to ? entries in the SQL returned
     * @return the TSQL to use in a WHERE part of a statement
     */
    public String toSql(
            final Class< ? extends DatabasePersistable > clazz, 
            final List< Object > sqlParameters );
}
