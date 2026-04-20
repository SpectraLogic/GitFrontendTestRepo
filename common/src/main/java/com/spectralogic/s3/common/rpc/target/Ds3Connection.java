/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import java.io.File;
import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.util.shutdown.Shutdownable;

/**
 * A connection to a remote DS3 endpoint.
 */
public interface Ds3Connection extends Shutdownable
{
    void verifyIsAdministrator();
    
    
    Set< DataPolicy > getDataPolicies();
    
    
    Set< User > getUsers();
    
    
    void createUser( final User user, final String dataPolicy );
    
    
    void updateUser( final User user );
    
    
    void deleteUser( final String userName );
    

    void createDs3Target( final Ds3Target target );
    
    
    DeleteObjectsResult deleteObjects( 
            final PreviousVersions previousVersions, 
            final Bucket bucket,
            final Collection< S3Object > objectIds );
    
	
    void undeleteObject( final S3Object object, final String bucketName );
    
	
    void deleteBucket( final String bucketName, final boolean deleteObjects );
    
    
    boolean isBucketExistant( final String bucketName );
    
    
    boolean isJobExistant( final UUID jobId );
    
    
    boolean isChunkAllocated( final UUID jobId, final UUID chunkId );


    List<UUID> getBlobsReady(final UUID jobId );


    /**
     * @return null if the chunk does not exist; false or true if the chunk does exist
     */
    Boolean getChunkReadyToRead( final UUID chunkId );
    
    
    void createBucket( final UUID bucketId, final String bucketName, final String dataPolicy );
    
    
    String GET_JOB_PREFIX = "Service ";
    
    
    UUID createGetJob(final Job sourceJob, final Collection< JobEntry> entries, final String bucketName );


    void verifySafeToCreatePutJob( final String bucketName );
    
    
    void replicatePutJob( final DetailedJobToReplicate jobToReplicate, final String bucketName );
    
    
    void cancelGetJob( final UUID sourceJobId );
    
    
    BlobPersistenceContainer getBlobPersistence( final UUID jobId, final Set< UUID > blobIds );
    
    
    void getBlob( 
            final UUID jobId,
            final String bucketName,
            final String objectName, 
            final Blob blob,
            final File fileInCache );
    
    
    void putBlob( 
            final UUID jobId,
            final String bucketName,
            final String objectName, 
            final Blob blob,
            final File fileInCache,
            final Date objectCreationDate,
            final Set< S3ObjectProperty > metadata );
    
    
    void keepJobAlive( final UUID jobId );
    
    
    int PREFERRED_WORK_WINDOW_NUMBER_OF_CHUNKS = 12;
    String DISABLE_TARGET_SOFTWARE_VERSION_CHECK = "disableTargetSoftwareVersionCheck";
}
