/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;

import java.util.List;

/**
 * A {@link PublicCloudConnection} where data can be offlined (for example, residing only on tape or other
 * offline media), requiring staging before it can be read from the cloud.
 */
public interface DataOfflineablePublicCloudConnection extends PublicCloudConnection
{
    /**
     * @return true if a call to {@link PublicCloudConnection#readBlobFromCloud} would succeed at this time;
     * The blob should be available and online. If the blob is on Glacier this call returns false.
     * false otherwise <br><br>
     */
    boolean isBlobReadyToBeReadFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object, 
            final Blob blob );


    /**
     * @return true if the blob is available on the cloud at this time;
     * false otherwise <br><br>
     */
    boolean isBlobAvailableOnCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob );
    /**
     * Begins the process of staging the specified blob in the cloud so that it can be read at a later time.
     */
    void beginStagingToRead( 
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object, 
            final Blob blob,
            final int stagedDataExpirationInDays );

}
