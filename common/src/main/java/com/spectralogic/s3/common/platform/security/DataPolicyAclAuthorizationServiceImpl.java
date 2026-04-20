/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.MutableCache;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

public final class DataPolicyAclAuthorizationServiceImpl implements DataPolicyAclAuthorizationService
{
    public DataPolicyAclAuthorizationServiceImpl( 
            final BeansRetrieverManager brm,
            final GroupMembershipCache groupMemberships,
            final int maxMillisToCache )
    {
        m_brm = brm;
        m_groupMemberships = groupMemberships;
        m_cache = new MutableCache<>( maxMillisToCache, new AccessRequestResultProvider() );
        Validations.verifyNotNull( "Retriever manager", m_brm );
        Validations.verifyNotNull( "Group memberships", m_groupMemberships );
    }
    
    
    public void verifyHasAccess( final DataPolicyAccessRequest request )
    {
        if ( !hasAccess( request ) )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.FORBIDDEN,
                    "User " + request.getUserId() + " does not have permission for data policy " 
                    + request.getDataPolicyId() + "." );
        }
    }
    
    
    public boolean hasAccess( final DataPolicyAccessRequest request )
    {
        if ( m_groupMemberships.isMember( request.getUserId(), BuiltInGroup.ADMINISTRATORS ) )
        {
            return true;
        }
        if ( AccessRequestResult.PERMIT == m_cache.get( request ) )
        {
            return true;
        }
        return false;
    }
    
    
    private final class AccessRequestResultProvider 
        implements CacheResultProvider< DataPolicyAccessRequest, AccessRequestResult >
    {
        public AccessRequestResult generateCacheResultFor( final DataPolicyAccessRequest request )
        {
            final Set< UUID > groupIds = m_groupMemberships.getGroups( request.getUserId() );
            final Set< DataPolicyAcl > acls = m_brm.getRetriever( DataPolicyAcl.class ).retrieveAll( 
                    Require.all( 
                            Require.any( 
                                    Require.beanPropertyEquals( 
                                            DataPolicyAcl.DATA_POLICY_ID, null ),
                                    Require.beanPropertyEquals(
                                            DataPolicyAcl.DATA_POLICY_ID, request.getDataPolicyId() ) ),
                            Require.any( 
                                    Require.beanPropertyEqualsOneOf(
                                            BucketAcl.GROUP_ID, groupIds ),
                                    Require.beanPropertyEquals( 
                                            UserIdObservable.USER_ID, request.getUserId() ) ) ) ).toSet();
            return ( acls.isEmpty() ) ?
                    AccessRequestResult.DENY
                    : AccessRequestResult.PERMIT;
        }
    } // end inner class def
    
    
    private enum AccessRequestResult
    {
        PERMIT,
        DENY
    } // end inner class def
    
    
    private final BeansRetrieverManager m_brm;
    private final GroupMembershipCache m_groupMemberships;
    private final MutableCache< DataPolicyAccessRequest, AccessRequestResult > m_cache;
}
