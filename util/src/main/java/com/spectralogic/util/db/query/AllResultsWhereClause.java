/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.List;

import com.spectralogic.util.db.lang.DatabasePersistable;

final class AllResultsWhereClause implements WhereClause
{
    private AllResultsWhereClause()
    {
        // singleton
    }
    
    
    public String toSql( 
            final Class< ? extends DatabasePersistable > clazz,
            final List< Object > sqlParameters )
    {
        return "true";
    }
    
    
    @Override
    public String toString()
    {
        return "{true}";
    }
    
    
    final static AllResultsWhereClause INSTANCE = new AllResultsWhereClause();
}
