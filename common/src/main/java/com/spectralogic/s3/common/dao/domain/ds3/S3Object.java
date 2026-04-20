/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

/**
 * A named data or folder entity that exists within a {@link Bucket}.  The object's name is guaranteed to be
 * unique within its bucket.
 */
@Indexes(
{
    @Index({ S3Object.BUCKET_ID, S3Object.NAME, S3Object.CREATION_DATE, S3Object.ID}), @Index({ S3Object.CREATION_DATE })
})
@ConcreteImplementation( S3ObjectImpl.class )
public interface S3Object extends DatabasePersistable
{
    String DELIMITER = "/";
    
        
    @SortBy( value = 3 )
    UUID getId();    
    
    
    String NAME = "name";

    @SortBy( 1 )
    String getName();
    
    S3Object setName( final String value );
    
    
    String TYPE = "type";
    
    S3ObjectType getType();
    
    S3Object setType( final S3ObjectType type );
    
    
    String LATEST = "latest";
    
    /**
     * @return TRUE if this object is the most recent version
     */
    @DefaultBooleanValue( false )
    boolean isLatest();
    
    S3Object setLatest( final boolean value );
    
    
    String BUCKET_ID = "bucketId";
    
    @References( Bucket.class )
    UUID getBucketId();
    
    S3Object setBucketId( final UUID bucketId );
    
    
    String CREATION_DATE = "creationDate";
    
    /**
     * @return the date the object was received in its entirety, or null if the object has not yet been
     * received in its entirety (the object is in the process of being created)
     */
    @Optional
    @SortBy( direction = Direction.DESCENDING, value = 2 )
    Date getCreationDate();
    
    S3Object setCreationDate( final Date value );
}
