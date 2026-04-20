/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.target;

import java.util.UUID;

import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;

public interface PublicCloudBucketSupport
{
    PublicCloudBucketInformation verifyBucketToImport( 
            final PublicCloudConnection connection, 
            final String cloudBucketName );

    PublicCloudBucketInformation verifyBucketForWrite(
            final PublicCloudConnection connection,
            final UUID bucketId,
            final UUID targetId );
    
    
    PublicCloudBucketInformation verifyBucket( 
            final PublicCloudConnection connection, 
            final UUID bucketId,
            final UUID targetId );
    
    
    String attainCloudBucketName( final UUID bucketId, final UUID targetId );
    

    int CLOUD_VERSION = 1;
}
