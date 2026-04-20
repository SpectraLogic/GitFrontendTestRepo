/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.orm.BucketRM;
import com.spectralogic.s3.common.dao.orm.JobEntryRM;
import com.spectralogic.s3.common.dao.orm.JobRM;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.lang.ByteRanges;

final class JobEntryServiceImpl
    extends BaseService<JobEntry> implements JobEntryService
{
    JobEntryServiceImpl()
    {
        super( JobEntry.class );
    }
    
    
    public int getNextChunkNumber( final UUID jobId )
    {
        return (int)getDataManager().getMax( 
                JobEntry.class,
                JobEntry.CHUNK_NUMBER,
                Require.beanPropertyEquals( JobEntry.JOB_ID, jobId ) ) + 1;
    }
    
    
    public long getSizeInBytes( final UUID chunkId )
    {
        return new JobEntryRM( chunkId, getServiceManager() ).getBlob().getLength();
    }

    public void verifyEntriesExist(final Collection<UUID> ids) {
        final int actual = getCount(Require.beanPropertyEqualsOneOf(Identifiable.ID, ids));
        if (ids.size() != actual) {
            throw new RuntimeException(actual + " out of " + ids.size() + " entries that were expected exist.");
        }
    }


    //Will return null if chunkID is not part of an IOM GET job
    public JobEntry getCounterpartPutChunk(final UUID chunkId ) {
        final JobEntry chunk = attain( chunkId );
        final UUID putJobId = getServiceManager().getService( JobService.class ).getPutJobComponentOfDataMigration(chunk.getJobId());
        if ( putJobId == null ) {
            return null;
        }
        return retrieve(
                Require.all(
                        Require.beanPropertyEquals( JobEntry.JOB_ID, putJobId ),
                        Require.beanPropertyEquals( JobEntry.CHUNK_NUMBER, chunk.getChunkNumber()) ) );
    }


    public JobEntry getEntryForS3Request(
            final JobRequestType requestType,
            final UUID objectId,
            final UUID jobId,
            final boolean includeNaked,
            final UUID blobId )
    {
        final S3Object object = getServiceManager().getRetriever( S3Object.class ).attain( objectId );
        final String objectName = object.getName();
        final String bucketName = new BucketRM( object.getBucketId(), getServiceManager() ).getName();

        WhereClause jobFilter =Require.beanPropertyEquals( JobObservable.REQUEST_TYPE, requestType );
        if ( null == jobId )
        {
            jobFilter = Require.all( jobFilter, Require.beanPropertyEquals( Job.IMPLICIT_JOB_ID_RESOLUTION, true ) );
        }
        else
        {
            final JobRM job = new JobRM( jobId, getServiceManager() );

            if ( job.getRequestType() != requestType )
            {
                throw new DaoException(
                        GenericFailure.BAD_REQUEST,
                        "Job " + jobId + " is a " + job.getRequestType() + " job , not a " + requestType + " job.");
            }
            jobFilter = Require.all( jobFilter, Require.beanPropertyEquals( Identifiable.ID, jobId ) );
        }
        if ( !includeNaked )
        {
            jobFilter = Require.all( jobFilter, Require.beanPropertyEquals( JobObservable.NAKED, false ) );
        }



        //NOTE: this query ran very slowly in EMPROD-4971. Filtering on job entry with job id existing shouldn't be
        // significantly different from filtering on job id directly, and was not able to reproduce problem, but could
        // be worth investigating.
        final List<JobEntry> entries = retrieveAll( Require.all(
                Require.exists( JobEntry.JOB_ID, jobFilter ),
                Require.beanPropertyEquals( JobEntry.BLOB_ID, blobId )) ).toList();
        if ( 1 != entries.size() )
        {
             if ( entries.isEmpty() )
            {
                /*throw new DaoException(
                        GenericFailure.CONFLICT,
                        "Object '" + objectName + "' in bucket '" + bucketName +
                                 " is not part of job " + jobId + "." );*/
                return null;
            }
            else
            {
                throw new DaoException(
                        GenericFailure.BAD_REQUEST,
                        "There are multiple active jobs for object '" + objectName + "' in bucket '"
                                + bucketName + "'. Please specify a job ID.");
            }
        }
        else
        {
            return entries.iterator().next();
        }
    }

    @Override
    public List<LocalBlobDestination> getTapeDestinations(UUID chunkId) {
        return getServiceManager().getRetriever(LocalBlobDestination.class).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals(LocalBlobDestination.ENTRY_ID, chunkId),
                        Require.exists(
                                LocalBlobDestination.PERSISTENCE_RULE_ID,
                                Require.exists(
                                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                                        Require.exists(
                                                StorageDomainMember.class,
                                                StorageDomainMember.STORAGE_DOMAIN_ID,
                                                Require.beanPropertyNotNull(StorageDomainMember.TAPE_PARTITION_ID)
                                        )
                                ))
                        )).toList();
    }

    @Override
    public List<LocalBlobDestination> getPoolDestinations(UUID chunkId) {
        return getServiceManager().getRetriever(LocalBlobDestination.class).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals(LocalBlobDestination.ENTRY_ID, chunkId),
                        Require.exists(
                                LocalBlobDestination.PERSISTENCE_RULE_ID,
                                Require.exists(
                                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                                        Require.exists(
                                                StorageDomainMember.class,
                                                StorageDomainMember.STORAGE_DOMAIN_ID,
                                                Require.beanPropertyNotNull(StorageDomainMember.POOL_PARTITION_ID)
                                        )
                                ))
                        )).toList();
    }

    @Override
    public List<Ds3BlobDestination> getDs3Destinations(UUID chunkId) {
        return getServiceManager().getRetriever(Ds3BlobDestination.class).retrieveAll(
                Require.beanPropertyEquals(RemoteBlobDestination.ENTRY_ID, chunkId)).toList();
    }

    @Override
    public List<AzureBlobDestination> getAzureDestinations(UUID chunkId) {
        return getServiceManager().getRetriever(AzureBlobDestination.class).retrieveAll(
                Require.beanPropertyEquals(RemoteBlobDestination.ENTRY_ID, chunkId)).toList();
    }

    @Override
    public List<S3BlobDestination> getS3Destinations(UUID chunkId) {
        return getServiceManager().getRetriever(S3BlobDestination.class).retrieveAll(
                Require.beanPropertyEquals(RemoteBlobDestination.ENTRY_ID, chunkId)).toList();
    }
}
