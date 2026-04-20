/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;

public interface CreatePutJobParams extends BaseCreateJobParams< CreatePutJobParams >
{
    String BLOBBING_POLICY = "blobbingPolicy";
    
    @DefaultEnumValue( "ENABLED" )
    BlobbingPolicy getBlobbingPolicy();
    
    CreatePutJobParams setBlobbingPolicy( final BlobbingPolicy value );
    
    
    String BUCKET_ID = "bucketId";
    
    UUID getBucketId();
    
    CreatePutJobParams setBucketId( final UUID value );
    
    
    String MAX_UPLOAD_SIZE_IN_BYTES = "maxUploadSizeInBytes";
    
    @Optional
    Long getMaxUploadSizeInBytes();
    
    CreatePutJobParams setMaxUploadSizeInBytes( final Long value );
    
    
    String MINIMIZE_SPANNING_ACROSS_MEDIA = "minimizeSpanningAcrossMedia";
    
    @DefaultBooleanValue( false )
    boolean isMinimizeSpanningAcrossMedia();
    
    CreatePutJobParams setMinimizeSpanningAcrossMedia( final boolean value );
    
    
    String IGNORE_NAMING_CONFLICTS = "ignoreNamingConflicts";

    @DefaultBooleanValue( false )
    boolean isIgnoreNamingConflicts();
    
    CreatePutJobParams setIgnoreNamingConflicts( final boolean value );
    
    
    String OBJECTS_TO_CREATE = "objectsToCreate";
    
    S3ObjectToCreate [] getObjectsToCreate();
    
    CreatePutJobParams setObjectsToCreate( final S3ObjectToCreate [] value );
    
    
    String FORCE = "force";
    
    boolean isForce();
    
    CreatePutJobParams setForce( final boolean value );
    

    String VERIFY_AFTER_WRITE = "verifyAfterWrite";
    
    boolean isVerifyAfterWrite();
    
    CreatePutJobParams setVerifyAfterWrite( final boolean value );
}
