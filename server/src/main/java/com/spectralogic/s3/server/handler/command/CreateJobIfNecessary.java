/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.command;

import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.orm.JobRM;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobbingPolicy;
import com.spectralogic.s3.server.domain.S3ObjectToJobApiBean;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.io.ByteRangesImpl;
import com.spectralogic.util.io.lang.ByteRanges;
import com.spectralogic.util.lang.CollectionFactory;

public final class CreateJobIfNecessary extends BaseCommand< UUID >
{
    public CreateJobIfNecessary( final JobRequestType type )
    {
        m_type = type;
    }


    @Override
    protected UUID executeInternal( final CommandExecutionParams params )
    {
    	 UUID jobId = ( params.getRequest().hasRequestParameter( RequestParameterType.JOB ) ) ?
                params.getRequest().getRequestParameter( RequestParameterType.JOB ).getUuid()
                : null;
    	final boolean cachedOnly = params.getRequest().hasRequestParameter( RequestParameterType.CACHED_ONLY );

    	if ( cachedOnly )
    	{
            //Check if there is an objectId based on the params.
            determineObjectInfo(params);
            BlobCache bc = params.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId( m_blobId);
            if ( bc == null || bc.getState() != CacheEntryState.IN_CACHE ) {
                if (cachedOnly) {
                    throw new S3RestException(
                            AWSFailure.SLOW_DOWN_BY_CLIENT,
                            "The object is not currently available in cache." );
                }
            }
            return determineObjectInfoFromExistingJob( params, true,  createNakedJob(params, true) );
        }
        try
        {
        	//NOTE: Naked puts should always start a new job unless specified explicitly.
        	//Naked gets might be using an existing job.
        	final boolean includeNaked = JobRequestType.GET == m_type || null != jobId;
            //Determine if there is an existing objectId associated with the jobId.
            //If the objectId is null, create a naked job and return the jobId.
            // If there is an existing objectId, then find the blobId.
            // If there is no blobId based on input params and objectId, then create a naked job.
            //If there is a blobId, then see if there is a job entry for the blobId.
            // If there is no job entry associated with the blobId, then create a naked job and return the jobId.
            UUID objectId = determineObjectId( params, jobId );
            if (objectId == null) {
                UUID nakedJobId = createNakedJob(params);
                determineObjectInfo(params,nakedJobId );
                return determineObjectInfoFromExistingJob(params, true, nakedJobId);
            } else {
                populateObjectInfoInternal(params, objectId);
                if (m_blobId == null) {
                    return checkAndCreateNakedJob( params );
                } else {
                    if (determineObjectInfoFromExistingJob( params, includeNaked, jobId ) == null) {
                        return checkAndCreateNakedJob( params );
                    }
                    return determineObjectInfoFromExistingJob( params, includeNaked, jobId );
                }
            }
        }
        catch ( final RuntimeException ex )
        {
            throw ex;

        }
    }



    private UUID checkAndCreateNakedJob( final CommandExecutionParams params )
    {
        UUID nakedJobId = createNakedJob(params);
        determineObjectInfo(params, nakedJobId);
        return determineObjectInfoFromExistingJob(params, true, nakedJobId);
    }


    private UUID createNakedJob( final CommandExecutionParams params )
    {
        return createNakedJob(params, false);
    }


    private UUID createNakedJob( final CommandExecutionParams params, boolean cachedOnly )
    {
        LOG.info( "Must create a naked job to service the jobless " + m_type + "." );
        final S3ObjectToJobApiBean objectToCreate = BeanFactory.newBean( S3ObjectToJobApiBean.class )
                .setName( params.getRequest().getObjectName() );
        if ( JobRequestType.PUT == m_type )
        {
            if ( params.getRequest().getHttpRequest().getType().equals( RequestType.POST )
                    && params.getRequest().hasRequestParameter( RequestParameterType.UPLOADS ) )
            {
                // It's a multi-part upload.
                objectToCreate.setSize( -1L );
            }
            else
            {
                objectToCreate.setSize( Long.parseLong( params.getRequest().getRequestHeader(
                        S3HeaderType.CONTENT_LENGTH ) ) );
            }
        }
        else
        {
            if ( params.getRequest().hasRequestParameter( RequestParameterType.VERSION_ID ) )
            {
                objectToCreate.setVersionId(
                		params.getRequest().getRequestParameter( RequestParameterType.VERSION_ID ).getUuid() );
            }
        }

        final Bucket bucket = params.getServiceManager().getRetriever( Bucket.class ).attain(
                Bucket.NAME, params.getRequest().getBucketName() );
        final Job job = BeanFactory.newBean( Job.class )
                .setRequestType( m_type )
                .setChunkClientProcessingOrderGuarantee( JobChunkClientProcessingOrderGuarantee.IN_ORDER )
                .setBucketId( bucket.getId() )
                .setAggregating( !cachedOnly )
                .setNaked( true );
        final CreateJob createJob = new CreateJob(
                job,
                BlobbingPolicy.DISABLED,
                objectToCreate );
        createJob.execute( params );

        return createJob.getJobId();
    }


    private UUID determineObjectInfoFromExistingJob(
    		final CommandExecutionParams params,
    		final boolean includeNaked,
    		final UUID jobId )
    {
        final JobEntry entry = params.getServiceManager().getService( JobEntryService.class )
        		.getEntryForS3Request(
        				m_type,
        				m_objectId,
        				jobId,
        				includeNaked,
        				m_blobId);
        return (entry != null) ? entry.getJobId() : null;
    }

    
    private UUID determineObjectId(final CommandExecutionParams params, UUID jobId) {
        //Job id resolution to determine if there is an existing objectId for the job.
       return params.getServiceManager().getService( S3ObjectService.class ).retrieveId(
                params.getRequest().getBucketName(),
                params.getRequest().getObjectName(),
                getVersion( params ),
               JobRequestType.PUT == m_type,
                jobId);

    }

    private void determineObjectInfo(final CommandExecutionParams params) {
        final UUID objectId = params.getServiceManager().getService( S3ObjectService.class ).retrieveId(
                params.getRequest().getBucketName(),
                params.getRequest().getObjectName(),
                getVersion( params ),
                JobRequestType.PUT == m_type);
        populateObjectInfoInternal(params, objectId);
    }

    
    private void determineObjectInfo(final CommandExecutionParams params, UUID jobId) {
        final UUID objectId = params.getServiceManager().getService( S3ObjectService.class ).retrieveId(
                params.getRequest().getBucketName(),
                params.getRequest().getObjectName(),
                getVersion( params ),
                JobRequestType.PUT == m_type,
                jobId);
        populateObjectInfoInternal(params, objectId);
    }

    private void populateObjectInfoInternal(final CommandExecutionParams params, final UUID objectId) {
        final Long offset = ( params.getRequest().hasRequestParameter( RequestParameterType.OFFSET ) ) ?
                Long.valueOf(
                        params.getRequest().getRequestParameter( RequestParameterType.OFFSET ).getLong() )
                : null;
        final String rawByteRanges = params.getRequest().getHttpRequest().getHeader(
                S3HeaderType.BYTE_RANGES );

        if ( null == objectId )
        {
            throw new DaoException(
                    GenericFailure.CONFLICT,
                    "Object '" + params.getRequest().getObjectName() + "' in bucket '"
                            + params.getRequest().getBucketName()
                            + "' with offset " + offset + " is not a valid object" );
        }
        final S3Object object = params.getServiceManager().getRetriever( S3Object.class ).attain( objectId );
        m_byteRanges = (null == rawByteRanges) ?
                null
                : new ByteRangesImpl(
                rawByteRanges,
                params.getServiceManager().getService(S3ObjectService.class)
                        .getSizeInBytes(CollectionFactory.toSet(objectId)));
        WhereClause offsetFilter;
        if ( null == offset )
        {
            offsetFilter = Require.nothing();
        }
        else
        {
            offsetFilter = Require.beanPropertyEquals( Blob.BYTE_OFFSET, offset );
        }
        final WhereClause rangeFilter = ( null == m_byteRanges ) ? null : Require.all(
                Require.beanPropertiesSumGreaterThan(
                        Blob.BYTE_OFFSET, Blob.LENGTH,
                        m_byteRanges.getFullRequiredRange().getStart() ),
                Require.beanPropertyLessThan(
                        Blob.BYTE_OFFSET,
                        m_byteRanges.getFullRequiredRange().getEnd() + 1 ) );
        final WhereClause blobFilter = Require.all(
                Require.beanPropertyEquals( Blob.OBJECT_ID, object.getId() ),
                offsetFilter,
                rangeFilter );

        final List< Blob > blobs = params.getServiceManager().getService( BlobService.class )
                .retrieveAll( blobFilter ).toList();
        if ( 1 != blobs.size() )
        {
            final Set< Long > offsets = BeanUtils.extractPropertyValues( blobs, Blob.BYTE_OFFSET );
            final List< Long > sortedOffsets = new ArrayList<>( offsets );
            Collections.sort( sortedOffsets );
            if (blobs.isEmpty())
            {
                throw new DaoException(
                        GenericFailure.NOT_FOUND,
                        "Object '" + params.getRequest().getObjectName() + "' in bucket '" + params.getRequest().getBucketName()
                                + "' exists, but the offset and/or byte ranges specified are invalid." );
            }
            else if ( null == offset && null == m_byteRanges )
            {
                throw new DaoException(
                        GenericFailure.BAD_REQUEST,
                        "Object '" + params.getRequest().getObjectName() + "' in bucket '" + params.getRequest().getBucketName()
                                + "' exists, but no offset or byte range was provided, which is required since the object"
                                + " has more than one blob. You may ask for any of these offsets: " + sortedOffsets );
            }
            else
            {
                throw new DaoException(
                        GenericFailure.BAD_REQUEST,
                        "Object '" + params.getRequest().getObjectName() + "' in bucket '" + params.getRequest().getBucketName()
                                + "' exists, but the requested byte ranges span more than one blob"
                                + " The offsets for those blobs are: " + sortedOffsets );
            }
        }

        m_offset = offset;
        m_objectId = objectId;
        m_blobId = blobs.get(0).getId();
    }

    private UUID getVersion( final CommandExecutionParams params )
    {
        if ( null != params.getRequest().getRequestParameter( RequestParameterType.VERSION_ID ) )
        {
            return params.getRequest().getRequestParameter( RequestParameterType.VERSION_ID ).getUuid();
        }
        return null;
    }

    public UUID getBlobId()
    {
        return m_blobId;
    }


    public ByteRanges getByteRanges()
    {
        return m_byteRanges;
    }

    private volatile UUID m_blobId;
    private volatile UUID m_objectId;
    private volatile Long m_offset;
    private volatile ByteRanges m_byteRanges;
    private final JobRequestType m_type;
}
