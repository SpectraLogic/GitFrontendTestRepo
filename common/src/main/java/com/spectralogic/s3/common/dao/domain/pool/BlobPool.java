/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.pool;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.Obsoletion;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;

public interface BlobPool extends BlobObservable< BlobPool >, DatabasePersistable
{
    String POOL_ID = "poolId";
    
    @References( Pool.class )
    UUID getPoolId();
    
    BlobPool setPoolId( final UUID value );
    
    
    String BUCKET_ID = "bucketId";
    
    @References( Bucket.class )
    @CascadeDelete
    UUID getBucketId();
    
    BlobPool setBucketId( final UUID value );
    
    
    String DATE_WRITTEN = "dateWritten";
    
    @DefaultToCurrentDate
    Date getDateWritten();
    
    BlobPool setDateWritten( final Date value );
    
    
    String LAST_ACCESSED = "lastAccessed";
    
    @DefaultToCurrentDate
    Date getLastAccessed();
    
    BlobPool setLastAccessed( final Date value );
}
