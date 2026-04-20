/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public interface S3ObjectService
    extends BeansRetriever< S3Object >, BeanUpdater< S3Object >
{
    int retrieveNumberOfObjectsInBucket( final UUID bucketId );
    
    
    public UUID retrieveId(
    		final String bucketName,
    		final String objectName,
    		final UUID versionId,
    		final boolean includeIncomplete,
    		final UUID jobId );
    
    
    UUID retrieveId(
    		final String bucketName,
    		final String objectName,
    		final UUID versionId,
    		final boolean includeIncomplete );
    		
    UUID attainId(
    		final String bucketName,
    		final String objectName,
    		final UUID versionId,
    		final boolean includeIncomplete );
    
    
    long getSizeInBytes( final Set< UUID > objectIds );
    
    
    long getSizeInBytes( final UUID objectId );
    
    
    boolean isEveryBlobReceived( final UUID objectId );


    void create( final Set< S3Object > objects );   
    
    
    Set< UUID > getObjectsForBlobs( final Set< UUID > blobIds );
      
    
    void markObjectReceived( final S3Object object );
        
        
    void markObjectReceived( final S3Object object, final Date creationDate );
    
    
    void markObjectsReceived( final Set< S3Object > objects );
    
    
    void deleteByBucketId( final UUID bucketId );
    
    
    S3Object getMostRecentVersion( final UUID bucketId, final String objectName );
    
    
    ReentrantReadWriteLock getLock();
    

    void deleteLegacyObjectsIfEntirelyPersisted( final Set< UUID > objectIds );
    
    
    DeleteResult delete( final PreviousVersions previousVersions, final Set< UUID > objectIds );
    
    
    DeleteResult delete(
            final PreviousVersions previousVersions, 
            final Set< UUID > objectIds,
            final DeleteS3ObjectsPreCommitListener preCommitListener );
    
    
    /**
     * Specifies the handling for previous versions of objects, if they exist.
     */
    enum PreviousVersions
    {
        UNMARK_LATEST,
        DELETE_SPECIFIC_VERSION, //NOTE: this will rollback to previous if possible
        DELETE_ALL_VERSIONS;

        public static PreviousVersions determineHandling(
                final boolean specificVersion,
                final VersioningLevel versioning) {
            return determineHandling( specificVersion, versioning, false );
        }

        public static PreviousVersions determineHandling(
                final boolean specificVersion,
                final VersioningLevel versioning,
                final boolean strictUnversionedDelete)
        {
            if ( specificVersion && !strictUnversionedDelete)
            {
                return DELETE_SPECIFIC_VERSION;
            }
            else if ( VersioningLevel.KEEP_MULTIPLE_VERSIONS == versioning )
            {
                return UNMARK_LATEST;
            }
            return DELETE_ALL_VERSIONS;
        }
    } // end inner class def
    
    
    interface DeleteS3ObjectsPreCommitListener
    {
        void preparedToCommitDelete( final BeansServiceManager transaction );
    } // end inner class def
    
    
    interface DeleteResult
    {
        DeleteObjectsResult toDeleteObjectsResult();
        Set< UUID > getBlobIds();
        String getBucketName();
        Set< UUID > getObjectIds();
    } // end inner class def
}
