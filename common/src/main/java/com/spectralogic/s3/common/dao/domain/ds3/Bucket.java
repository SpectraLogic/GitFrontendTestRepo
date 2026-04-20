/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.MustMatchRegularExpression;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;
import com.spectralogic.util.db.lang.shared.ExcludeFromDatabasePersistence;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

/**
 * A logical container for {@link S3Object}s that defines various policies for its {@link S3Object}s and their
 * {@link Blob}s.
 */
@UniqueIndexes(
{
    @Unique( Bucket.NAME )
})
public interface Bucket extends DatabasePersistable, UserIdObservable< Bucket >
{
    String NAME = "name";
    
    @SortBy
    @MustMatchRegularExpression( "[A-Za-z0-9\\.\\-\\_]{1,63}" )
    String getName();
    
    Bucket setName( final String value );
    
    
    String CREATION_DATE = "creationDate";
    
    @DefaultToCurrentDate
    Date getCreationDate();
    
    Bucket setCreationDate( final Date value );
    
    
    String DATA_POLICY_ID = "dataPolicyId";
    
    @References( DataPolicy.class )
    UUID getDataPolicyId();
    
    Bucket setDataPolicyId( final UUID value );


    String LAST_PREFERRED_CHUNK_SIZE_IN_BYTES = "lastPreferredChunkSizeInBytes";

    /**
     * @return the last preferred chunk size computed for a PUT job generated for this bucket
     */
    @Optional
    Long getLastPreferredChunkSizeInBytes();

    Bucket setLastPreferredChunkSizeInBytes( final Long value );


    String LOGICAL_USED_CAPACITY = "logicalUsedCapacity";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @ExcludeFromDatabasePersistence
    Long getLogicalUsedCapacity();
    
    Bucket setLogicalUsedCapacity( final Long value );
    
    
    String EMPTY = "empty";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @ExcludeFromDatabasePersistence
    Boolean getEmpty();
    
    Bucket setEmpty( final Boolean value );


    String PROTECTED = "protected";

    /**
     * @return true if this is a protected bucket that cannot be deleted. This is used by clients as a safeguard to
     * prevent accidental deletion of important buckets.
     */
    @DefaultBooleanValue( false )
    boolean isProtected();

    Bucket setProtected( final boolean value );
}
