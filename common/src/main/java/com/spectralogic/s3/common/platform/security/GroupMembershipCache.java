/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

public final class GroupMembershipCache
{
    public GroupMembershipCache( 
            final BeansServiceManager serviceManager,
            final int maxMillisToCache )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        m_brm = serviceManager;
        m_service = serviceManager.getService( GroupService.class );
        m_maxMillisToCache = maxMillisToCache;
        Validations.verifyInRange( "Max millis to cache", 1, 1000 * 3600, m_maxMillisToCache );
    }
    
    
    synchronized public void invalidate()
    {
        m_durationCached = null;
    }
    
    
    synchronized public Set< UUID > getGroups( final UUID userId )
    {
        verifyCacheValid();
        try
        {
            return m_memberships.getGroups( userId );
        }
        catch ( final RuntimeException ex )
        {
            Validations.verifyNotNull( "Cache must be stale.", ex );
            return new HashSet<>();
        }
    }
    
    
    public boolean isMember( final UUID userId, final BuiltInGroup group )
    {
        return ( getGroups( userId ).contains( m_service.getBuiltInGroup( group ).getId() ) );
    }
    
    
    private void verifyCacheValid()
    {
        if ( null != m_durationCached && m_durationCached.getElapsedMillis() > m_maxMillisToCache )
        {
            m_durationCached = null;
        }
        if ( null != m_durationCached )
        {
            return;
        }
        
        m_durationCached = new Duration();
        m_memberships = new GroupMembershipCalculator( m_brm );
    }
    
    
    private Duration m_durationCached;
    private GroupMembershipCalculator m_memberships;
    private final GroupService m_service;
    private final BeansRetrieverManager m_brm;
    private final int m_maxMillisToCache;
}
