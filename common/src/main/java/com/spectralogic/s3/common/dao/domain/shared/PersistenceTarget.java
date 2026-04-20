/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.References;

public interface PersistenceTarget< T > extends SimpleBeanSafeToProxy
{
    String BUCKET_ID = "bucketId";
    
    /**
     * @return the bucket that owns this allocatable storage unit (if the bucket is using bucket data 
     * isolation mode), or null if the allocatable storage unit is not owned by a specific bucket
     */
    @Optional
    @References( Bucket.class )
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    UUID getBucketId();
    
    T setBucketId( final UUID bucketId );
    
    
    String STORAGE_DOMAIN_MEMBER_ID = "storageDomainMemberId";
    
    /**
     * @return the storage domain member ID that links this allocatable storage unit to a storage domain (or null if
     * the allocatable storage unit is unowned)
     */
    @Optional
    @References( StorageDomainMember.class )
    UUID getStorageDomainMemberId();
    
    T setStorageDomainMemberId( final UUID value );
    
    
    String ASSIGNED_TO_STORAGE_DOMAIN = "assignedToStorageDomain";
    
    /**
     * @return true if this allocatable storage unit has been assigned to a storage domain.  If true is 
     * returned but the storage domain id is null that means that the storage domain this allocatable storage 
     * unit was assigned to has been deleted and this allocatable storage unit must be formatted before it can
     * be allocated to another storage domain.
     */
    @DefaultBooleanValue( false )
    boolean isAssignedToStorageDomain();
    
    T setAssignedToStorageDomain( final boolean value );
    
    
    String LAST_ACCESSED = "lastAccessed";

    /**
     * @return the date we most recently accessed the {@link PersistenceTarget}
     */
    @Optional
    Date getLastAccessed();
    
    T setLastAccessed( final Date value );
    
    
    String LAST_VERIFIED = "lastVerified";

    /**
     * @return the date we most recently verified we could read all the contents on the 
     * {@link PersistenceTarget} and verify that the contents read matched their corresponding checksums <br>
     * 
     * Note: This date is reset to null whenever the tape is modified
     */
    @Optional
    Date getLastVerified();
    
    T setLastVerified( final Date value );
    
    
    String LAST_MODIFIED = "lastModified";

    /**
     * @return the date we most recently modified the contents of the {@link PersistenceTarget}
     */
    @Optional
    Date getLastModified();
    
    T setLastModified( final Date value );
}
