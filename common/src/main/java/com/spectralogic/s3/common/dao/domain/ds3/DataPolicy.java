/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;
import com.spectralogic.util.security.ChecksumType;

/**
 * Policy for data placement, data integrity, object spanning, default job configurations, etc. that any 
 * number of {@link Bucket}s can use.
 */
@UniqueIndexes(
{
    @Unique( Bucket.NAME )
})
public interface DataPolicy extends NameObservable< DataPolicy >, DatabasePersistable
{
    String CREATION_DATE = "creationDate";
    
    @DefaultToCurrentDate
    Date getCreationDate();
    
    DataPolicy setCreationDate( final Date value );
    
    
    String DEFAULT_GET_JOB_PRIORITY = "defaultGetJobPriority";

    @DefaultEnumValue( "HIGH" )
    BlobStoreTaskPriority getDefaultGetJobPriority();
    
    DataPolicy setDefaultGetJobPriority( final BlobStoreTaskPriority value );
    
    
    String DEFAULT_VERIFY_JOB_PRIORITY = "defaultVerifyJobPriority";

    @DefaultEnumValue( "LOW" )
    BlobStoreTaskPriority getDefaultVerifyJobPriority();
    
    DataPolicy setDefaultVerifyJobPriority( final BlobStoreTaskPriority value );
    
    
    String DEFAULT_PUT_JOB_PRIORITY = "defaultPutJobPriority";
    
    @DefaultEnumValue( "NORMAL" )
    BlobStoreTaskPriority getDefaultPutJobPriority();
    
    DataPolicy setDefaultPutJobPriority( final BlobStoreTaskPriority value );
    
    
    String DEFAULT_VERIFY_AFTER_WRITE = "defaultVerifyAfterWrite";
    
    @DefaultBooleanValue( false )
    boolean isDefaultVerifyAfterWrite();
    
    DataPolicy setDefaultVerifyAfterWrite( final boolean value );
    
    
    String CHECKSUM_TYPE = "checksumType";

    /**
     * @return the {@link ChecksumType} to use for {@link Blob}s that are a member of a {@link Bucket} that
     * uses this {@link DataPolicy}.  This value cannot be changed once {@link Blob}s have been written
     * against this {@link DataPolicy}.
     */
    @DefaultEnumValue( "MD5" )
    ChecksumType getChecksumType();
    
    DataPolicy setChecksumType( final ChecksumType value );
    
    
    String END_TO_END_CRC_REQUIRED = "endToEndCrcRequired";
    
    @DefaultBooleanValue( false )
    boolean isEndToEndCrcRequired();
    
    DataPolicy setEndToEndCrcRequired( final boolean value );
    
    
    String REBUILD_PRIORITY = "rebuildPriority";
    
    @DefaultEnumValue( "LOW" )
    BlobStoreTaskPriority getRebuildPriority();
    
    DataPolicy setRebuildPriority( final BlobStoreTaskPriority value );
    
    
    String DEFAULT_BLOB_SIZE = "defaultBlobSize";
    
    @Optional
    Long getDefaultBlobSize();
    
    DataPolicy setDefaultBlobSize( final Long value );
    
    
    String BLOBBING_ENABLED = "blobbingEnabled";
    
    /**
     * @return TRUE if an object is permitted to be "blobbed up" into multiple blobs (if FALSE, an 
     * {@link S3Object} can only have a single {@link Blob})  <br><br>
     * 
     * Note: Disabling blobbing ensures that objects will never span across media.
     */
    @DefaultBooleanValue( true )
    boolean isBlobbingEnabled();
    
    DataPolicy setBlobbingEnabled( final boolean value );
            
    
    String VERSIONING = "versioning";
    
    @DefaultEnumValue( "NONE" )
    VersioningLevel getVersioning();
    
    DataPolicy setVersioning( final VersioningLevel value );
    
    
    String MAX_VERSIONS_TO_KEEP = "maxVersionsToKeep";
    
    @DefaultIntegerValue( 1000 )
    int getMaxVersionsToKeep();
    
    DataPolicy setMaxVersionsToKeep( final int value );
    
    
    String ALWAYS_FORCE_PUT_JOB_CREATION = "alwaysForcePutJobCreation";

    @DefaultBooleanValue( false )
    boolean isAlwaysForcePutJobCreation();
    
    DataPolicy setAlwaysForcePutJobCreation( final boolean value );
    
    
    String ALWAYS_MINIMIZE_SPANNING_ACROSS_MEDIA = "alwaysMinimizeSpanningAcrossMedia";
    
    @DefaultBooleanValue( false )
    boolean isAlwaysMinimizeSpanningAcrossMedia();
    
    DataPolicy setAlwaysMinimizeSpanningAcrossMedia( final boolean value );
}
