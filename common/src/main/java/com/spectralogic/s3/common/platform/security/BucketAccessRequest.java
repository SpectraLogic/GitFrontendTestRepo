/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.util.lang.Validations;

public final class BucketAccessRequest
{
    public BucketAccessRequest( 
            final UUID userId, final UUID bucketId, final BucketAclPermission permissionRequired )
    {
        m_userId = userId;
        m_bucketId = bucketId;
        m_permissionRequired = permissionRequired;
        Validations.verifyNotNull( "User id", m_userId );
        Validations.verifyNotNull( "Bucket id", m_bucketId );
        Validations.verifyNotNull( "Permission required", m_permissionRequired );
    }
    
    
    public UUID getUserId()
    {
        return m_userId;
    }
    
    
    public UUID getBucketId()
    {
        return m_bucketId;
    }
    
    
    public BucketAclPermission getPermissionRequired()
    {
        return m_permissionRequired;
    }
    
    
    @Override
    public int hashCode()
    {
        return m_userId.hashCode() + m_bucketId.hashCode() + m_permissionRequired.hashCode();
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
        if ( ! ( other instanceof BucketAccessRequest ) )
        {
            return false;
        }
        
        final BucketAccessRequest oth = (BucketAccessRequest)other;
        return ( m_userId.equals( oth.m_userId ) 
                && m_bucketId.equals( oth.m_bucketId )
                && m_permissionRequired.equals( oth.m_permissionRequired ) );
    }


    private final UUID m_userId;
    private final UUID m_bucketId;
    private final BucketAclPermission m_permissionRequired;
}