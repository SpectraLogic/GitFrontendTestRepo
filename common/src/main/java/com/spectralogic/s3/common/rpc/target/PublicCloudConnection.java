/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.util.shutdown.Shutdownable;

public interface PublicCloudConnection extends Shutdownable
{
    /**
     * Verifies that a connection can be made with the cloud target and that the user credentials are valid.
     */
    void verifyConnectivity();
    
    
    /**
     * @return null if the bucket doesn't exist, non-null if it does <br><br>
     * 
     * If this method returns non-null but the properties in the {@link PublicCloudBucketInformation} aren't
     * populated, then the bucket exists, but does not appear to be a Black Pearl compatible bucket and should
     * not be used by the client invoking this method as a Black Pearl compatible bucket.
     */
    PublicCloudBucketInformation getExistingBucketInformation( final String bucketName );
    
    
    /**
     * Creates the specified bucket.
     */
    PublicCloudBucketInformation createOrTakeoverBucket(
            final Object initialDataPlacement,
            final PublicCloudBucketInformation cloudBucket );
    
    
    /**
     * Takes ownership over an existing bucket.
     */
    PublicCloudBucketInformation takeOwnership( 
            final PublicCloudBucketInformation cloudBucket,
            final UUID newOwnerId );
    
    
    /**
     * Retrieves the blob from the cloud target and writes it to the local <code>fileInCache</code>.
     */
    List< Future< ? > > readBlobFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object, 
            final Blob blob,
            final File fileInCache );
    
    
    /**
     * Replicates the blob, skipping over replication steps unnecessary due to the step already being
     * replicated.  <br><br>
     * 
     * Note: If a blob has been partially replicated, it is up to the implementation of this method to decide 
     * whether to delete the partial replication and re-replicate everything or pick up where it left off in 
     * the partial replication.
     */
    List< Future< ? > > writeBlobToCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final long objectSize,
            final Blob blob,
            final int numBlobsForObject,
            final File fileInCache,
            final Date objectCreationDate,
            final Set< S3ObjectProperty > metadata,
            final long maxBlobPartLength,
            final Object initialDataPlacement );
    
    
    /**
     * @param marker - if null, will search for contents from the beginning; if non-null, will search for
     * contents from the designated marker
     */
    BucketOnPublicCloud discoverContents( 
            final PublicCloudBucketInformation cloudBucket, 
            final String marker );
    
    
    /**
     * Deletes all metadata, indexes, and data relating to the specified object ids
     */
    void delete( final PublicCloudBucketInformation cloudBucket, final Set< S3ObjectOnMedia > objectIds );
    
    
    /**
     * Deletes the specified bucket, along with all its contents. We may or may not throw an exception
     * if the specified bucket does not exist, depending on the target type.
     */
    void deleteBucket( final String bucketName );
   
    
    /*
     * Will cancel any unknown multipart uploads or equivalent, as well as delete any from our database that track
     * uploads no longer visible on cloud. This operation may block the completion or creation of new uploads while
     * in progress.
     */
    void syncUploads( final String cloudBucketName );
    
    
    int MAX_CONNECTIONS = 50;
    int MAX_THREADS = 10;
}
