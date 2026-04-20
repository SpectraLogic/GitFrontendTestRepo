/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.List;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.lang.Validations;

final class NegationWhereClause implements WhereClause
{
    NegationWhereClause( final WhereClause whereClause )
    {
        m_whereClause = whereClause;
        Validations.verifyNotNull( "Where clause", whereClause );
    }
    

    public String toSql( 
            final Class< ? extends DatabasePersistable > clazz, 
            final List< Object > sqlParameters )
    {
        return "NOT " + m_whereClause.toSql( clazz, sqlParameters );
    }
    
    
    @Override
    public String toString()
    {
        return "NOT " + m_whereClause.toString();
    }
    

    private final WhereClause m_whereClause;
}
