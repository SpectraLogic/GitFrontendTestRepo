/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.util.List;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

public final class CronRunnableIdentifier
{
    public CronRunnableIdentifier( final Object ... identifierParts )
    {
        m_identifier = CollectionFactory.toList( identifierParts );
        Validations.verifyInRange( "Number of identifier parts", 1, 10, m_identifier.size() );
        if ( m_identifier.contains( null ) )
        {
            throw new IllegalArgumentException( "Identifier parts may not include null." );
        }
    }
    
    
    @Override
    public int hashCode()
    {
        int retval = 0;
        for ( final Object id : m_identifier )
        {
            retval += id.hashCode();
        }
        return retval;
    }


    @Override
    public boolean equals( final Object other )
    {
        if ( this == other )
        {
            return true;
        }
        if ( null == other )
        {
            return false;
        }
        if ( !( other instanceof CronRunnableIdentifier ) )
        {
            return false;
        }
        
        final CronRunnableIdentifier identifier = (CronRunnableIdentifier)other;
        if ( m_identifier.size() != identifier.m_identifier.size() )
        {
            return false;
        }
        for ( int i = 0; i < m_identifier.size(); ++i )
        {
            if ( !m_identifier.get( i ).equals( identifier.m_identifier.get( i ) ) )
            {
                return false;
            }
        }
        return true;
    }
    
    
    @Override
    public String toString()
    {
        String retval = "";
        for ( final Object id : m_identifier )
        {
            if ( retval.isEmpty() )
            {
                retval += "CronJob[";
            }
            else
            {
                retval += "|";
            }
            
            if ( Class.class.isAssignableFrom( id.getClass() ) )
            {
                retval += ( (Class< ? >)id ).getSimpleName();
            }
            else
            {
                retval += id;
            }
        }
        
        return retval + "]";
    }


    private final List< Object > m_identifier;
} // end inner class def