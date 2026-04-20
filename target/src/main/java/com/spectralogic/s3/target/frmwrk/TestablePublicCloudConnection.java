/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.frmwrk;



public interface TestablePublicCloudConnection
{
    void createGenericBucket( final String name ) throws Exception;
       
    
    void deleteOldBuckets( final long maxAgeInMillis, final String bucketNamePrefix );
}
