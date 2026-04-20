/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface PublicCloudBucketInformation 
    extends NameObservable< PublicCloudBucketInformation >, SimpleBeanSafeToProxy
{
    String OWNER_ID = "ownerId";
    
    UUID getOwnerId();
    
    PublicCloudBucketInformation setOwnerId( final UUID value );
    
    
    String LOCAL_BUCKET_NAME = "localBucketName";
    
    String getLocalBucketName();
    
    PublicCloudBucketInformation setLocalBucketName( final String value );
    
    
    String VERSION = "version";
    
    int getVersion();
    
    PublicCloudBucketInformation setVersion( final int value );
}
