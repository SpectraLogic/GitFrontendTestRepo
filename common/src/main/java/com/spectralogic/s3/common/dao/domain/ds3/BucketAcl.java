/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique({ BucketAcl.BUCKET_ID, BucketAcl.GROUP_ID, BucketAcl.PERMISSION }),
    @Unique({ BucketAcl.BUCKET_ID, UserIdObservable.USER_ID, BucketAcl.PERMISSION })
})
public interface BucketAcl extends DatabasePersistable, UserIdObservable< BucketAcl >
{
    String BUCKET_ID = "bucketId";
    
    /**
     * If null, the ACL applies globally across all buckets.  If not null, the ACL applies only to the bucket
     * specified.
     */
    @Optional
    @References( Bucket.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getBucketId();
    
    BucketAcl setBucketId( final UUID value );
    
    
    /**
     * The user that receives the access.  If the user is specified, the group cannot be specified and vice
     * versa.  Either the user or group must be set.
     */
    @Optional
    @References( User.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getUserId();
    
    
    String GROUP_ID = "groupId";

    /**
     * The user that receives the access.  If the user is specified, the group cannot be specified and vice
     * versa.  Either the user or group must be set.
     */
    @Optional
    @References( Group.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getGroupId();
    
    BucketAcl setGroupId( final UUID value );
    
    
    /**
     * The permission to provide.
     */
    String PERMISSION = "permission";
    
    BucketAclPermission getPermission();
    
    BucketAcl setPermission( final BucketAclPermission permission );
}
