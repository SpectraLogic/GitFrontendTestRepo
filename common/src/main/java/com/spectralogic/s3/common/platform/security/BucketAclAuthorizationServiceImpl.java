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
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.MutableCache;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

public final class BucketAclAuthorizationServiceImpl implements BucketAclAuthorizationService
{
    public BucketAclAuthorizationServiceImpl( 
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
    
    
    public void verifyHasAccess(
            final BucketAccessRequest request, 
            final AdministratorOverride administratorOverride )
    {
        if ( !hasAccess( request, administratorOverride ) )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.FORBIDDEN,
                    "User " + request.getUserId() + " does not have " + request.getPermissionRequired()
                    + " permission for bucket " + request.getBucketId() + "." );
        }
    }
    
    
    public boolean hasAccess( 
            final BucketAccessRequest request, 
            final AdministratorOverride administratorOverride )
    {
        if ( AdministratorOverride.YES == administratorOverride
                && m_groupMemberships.isMember( request.getUserId(), BuiltInGroup.ADMINISTRATORS ) )
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
        implements CacheResultProvider< BucketAccessRequest, AccessRequestResult >
    {
        public AccessRequestResult generateCacheResultFor( final BucketAccessRequest request )
        {
            final Set< UUID > groupIds = m_groupMemberships.getGroups( request.getUserId() );
            final Set< BucketAcl > acls = m_brm.getRetriever( BucketAcl.class ).retrieveAll( Require.all( 
                    Require.any( 
                            Require.beanPropertyEquals( BucketAcl.BUCKET_ID, null ),
                            Require.beanPropertyEquals( BucketAcl.BUCKET_ID, request.getBucketId() ) ),
                    Require.any( 
                            Require.beanPropertyEqualsOneOf( BucketAcl.GROUP_ID, groupIds ),
                            Require.beanPropertyEquals( UserIdObservable.USER_ID, request.getUserId() ) ),
                    Require.beanPropertyEqualsOneOf( 
                            BucketAcl.PERMISSION,
                            request.getPermissionRequired(), 
                            BucketAclPermission.OWNER ) ) ).toSet();
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
    private final MutableCache< BucketAccessRequest, AccessRequestResult > m_cache;
}
