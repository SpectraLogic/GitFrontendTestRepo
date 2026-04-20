/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.lang.Duration;

/**
 * Reduces the severity and successfulness of client-based brute force dictionary attacks on user accounts to
 * get the password and, to a lesser extent, denial of service attacks.
 */
public final class ClientAttackMitigator
{
    public ClientAttackMitigator()
    {
        this( 50, 10000 );
    }
    
    
    ClientAttackMitigator(
            final int maxAuthorizationFailures,
            final int authorizationFailureTtlInMillis )
    {
        m_maxAuthorizationFailures = 
                maxAuthorizationFailures;
        m_authorizationFailureTtlInMillis = 
                authorizationFailureTtlInMillis;
    }
    
    
    public boolean isAttackFromClient( final HttpRequest request )
    {
        return isAttack( m_clientFailures, request.getRemoteAddr() );
    }
    
    
    public boolean isAttackOnUser( final String user )
    {
        return isAttack( m_userFailures, user );
    }
    
    
    synchronized private boolean isAttack( final Map< ?, Set< Duration > > failures, final Object key )
    {
        if ( !failures.containsKey( key ) )
        {
            return false;
        }
        
        return isTooManyAttempts( failures.get( key ) );
    }
    
    
    private void cleanUpStaleFailures( final Set< Duration > durations )
    {
        for ( final Duration duration : new HashSet<>( durations ) )
        {
            if ( duration.getElapsedMillis() > m_authorizationFailureTtlInMillis )
            {
                durations.remove( duration );
            }
        }
    }
    
    
    private boolean isTooManyAttempts( final Set< Duration > durations )
    {
        if ( null == durations )
        {
            return false;
        }
        
        cleanUpStaleFailures( durations );
        if ( durations.size() > m_maxAuthorizationFailures )
        {
            return true;
        }
        
        return false;
    }
    
    
    public void authorizationFailed( final String user, final HttpRequest request )
    {
        final String client = request.getRemoteAddr();
        synchronized ( this )
        {
            if ( !m_clientFailures.containsKey( client ) )
            {
                m_clientFailures.put( client, new HashSet< Duration >() );
            }
            m_clientFailures.get( client ).add( new Duration() );
            
            if ( !m_userFailures.containsKey( user ) )
            {
                m_userFailures.put( user, new HashSet< Duration >() );
            }
            m_userFailures.get( user ).add( new Duration() );
        }
    }
    
    
    private final Map< String, Set< Duration > > m_clientFailures = new HashMap<>();
    private final Map< String, Set< Duration > > m_userFailures = new HashMap<>();
    
    private final int m_maxAuthorizationFailures;
    private final int m_authorizationFailureTtlInMillis;
}
