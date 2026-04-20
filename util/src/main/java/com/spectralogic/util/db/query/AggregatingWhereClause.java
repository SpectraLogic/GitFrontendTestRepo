/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.List;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

final class AggregatingWhereClause implements WhereClause
{
    enum AggregationType
    {
        AND( "AND", "true" ),
        OR( "OR", "false" ),
        ;
        
        private AggregationType( final String joinWord, final String emptyClausesWord )
        {
            m_joinWord = joinWord;
            m_emptyClausesWord = emptyClausesWord;
        }
        
        private final String m_joinWord;
        private final String m_emptyClausesWord;
    }
    
    
    AggregatingWhereClause( final AggregationType type, final WhereClause ... whereClausesToCompoundTogether )
    {
        m_type = type;
        m_clauses = CollectionFactory.toList( whereClausesToCompoundTogether );
        while ( m_clauses.contains( null ) )
        {
            m_clauses.remove( null );
        }
        
        Validations.verifyNotNull( "Aggregation type", m_type );
    }

    
    public String toSql( 
            final Class< ? extends DatabasePersistable > clazz,
            final List< Object > sqlParameters )
    {
        final StringBuilder retval = new StringBuilder();
        
        retval.append( "(" );
        boolean isFirst = true;
        for ( final WhereClause clause : m_clauses )
        {
            final String sql = clause.toSql( clazz, sqlParameters );
            if ( null == sql || sql.isEmpty() )
            {
                continue;
            }
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                retval.append( " " + m_type.m_joinWord + " " );
            }
            retval.append( sql );
        }
        retval.append( ")" );
        
        if ( isFirst )
        {
            return m_type.m_emptyClausesWord;
        }
        
        return retval.toString();
    }
    
    
    @Override
    public String toString()
    {
        final StringBuilder retval = new StringBuilder();
        boolean first = true;
        for ( final WhereClause clause : m_clauses )
        {
            if ( first )
            {
                first = false;
            }
            else
            {
                retval.append( " " ).append( m_type.m_joinWord ).append( " " );
            }
            retval.append( clause );
        }
        return retval.toString();
    }
    

    private final List< WhereClause > m_clauses;
    private final AggregationType m_type;
}
