/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

import com.spectralogic.util.tunables.Tunables;

public final class GroupMembershipCalculator
{
    public GroupMembershipCalculator( final BeansRetrieverManager brm )
    {
        m_brm = brm;
        Validations.verifyNotNull( "Beans retriever manager", brm );
    }
    
    
    synchronized public GroupMembershipCalculator addGroupMember( final GroupMember member )
    {
        Validations.verifyNotNull( "Member", member );
        if ( null != m_memberships )
        {
            throw new IllegalStateException( "Already calculated effective memberships." );
        }
        
        m_addedMembers.add( member );
        return this;
    }
    
    
    public Set< UUID > getGroups( final UUID userId )
    {
        calculate();
        if ( !m_memberships.containsKey( userId ) )
        {
            throw new IllegalArgumentException( "User id unknown: " + userId );
        }
        
        return new HashSet<>( m_memberships.get( userId ) );
    }
    
    
    synchronized public void calculate()
    {
        if ( null != m_memberships )
        {
            return;
        }
        
        m_memberships = new HashMap<>();
        try
        {
            new EffectiveMembershipCalculator().calculate( Tunables.groupMembershipCalculatorMaxRecursiveDepthAllowed() );
        }
        catch ( final RuntimeException ex )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT, "Effective group memberships failed validation.", ex );
        }
    }
    
    
    private final class EffectiveMembershipCalculator
    {
        private EffectiveMembershipCalculator()
        {
            for ( final Group group : m_brm.getRetriever( Group.class ).retrieveAll().toSet() )
            {
                m_groups.put( group.getId(), new HashSet< UUID >() );
            }
            m_members = m_brm.getRetriever( GroupMember.class ).retrieveAll().toSet();
            m_members.addAll( m_addedMembers );
        }
        
        
        private void calculate( final int remainingRecusiveDepth )
        {
            // Calculate the immediate members of groups without nesting
            for ( final UUID groupId : m_groups.keySet() )
            {
                for ( final UUID userId : calculate( groupId ) )
                {
                    if ( !m_memberships.containsKey( userId ) )
                    {
                        m_memberships.put( userId, new HashSet< UUID >() );
                    }
                    m_memberships.get( userId ).add( groupId );
                    m_groups.get( groupId ).add( userId );
                }
            }
            
            // Calculate nested members of groups
            for ( final UUID groupId : m_groups.keySet() )
            {
                for ( final UUID userId : calculate( groupId, remainingRecusiveDepth ) )
                {
                    m_memberships.get( userId ).add( groupId );
                }
            }
        }
        
        
        private Set< UUID > calculate( final UUID groupId )
        {
            final Set< UUID > retval = new HashSet<>();
            for ( final GroupMember member : m_members )
            {
                if ( groupId.equals( member.getGroupId() ) )
                {
                    retval.add( member.getMemberUserId() );
                }
            }
            retval.remove( null );
            return retval;
        }
        
        
        private Set< UUID > calculate(
                final UUID groupId,
                final int remainingRecusiveDepth )
        {
            if ( 0 > remainingRecusiveDepth )
            {
                throw new RuntimeException(
                        "The maximum recursive depth has been reached " 
                        + "attempting to determine effective group memberships." );
            }
            
            final Set< UUID > retval = new HashSet<>( m_groups.get( groupId ) );
            for ( final GroupMember member : m_members )
            {
                if ( groupId.equals( member.getGroupId() ) && null != member.getMemberGroupId() )
                {
                    retval.addAll( calculate(
                            member.getMemberGroupId(), 
                            remainingRecusiveDepth - 1 ) );
                }
            }
            return retval;
        }


        private final Set< GroupMember > m_members;
        private final Map< UUID, Set< UUID > > m_groups = new HashMap<>();
    } // end inner class def
    
    
    private Map< UUID, Set< UUID > > m_memberships;
    private final Set< GroupMember > m_addedMembers = new HashSet<>();
    private final BeansRetrieverManager m_brm;
    
}
