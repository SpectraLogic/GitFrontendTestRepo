/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.orm.BlobRM;
import com.spectralogic.s3.common.dao.orm.BucketRM;
import com.spectralogic.s3.common.dao.orm.JobRM;
import com.spectralogic.s3.common.dao.orm.S3ObjectRM;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.aws.S3Utils;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.security.ChecksumType;

public class CompleteBlobRequestHandler extends BaseRequestHandler

{
    public CompleteBlobRequestHandler()
    {
        super( new InternalAccessOnlyAuthenticationStrategy(),
                new NonRestfulCanHandleRequestDeterminer( RequestType.POST, BucketRequirement.REQUIRED,
                        S3ObjectRequirement.REQUIRED ) );

        registerRequiredRequestParameters( RequestParameterType.JOB, RequestParameterType.BLOB );
        registerOptionalRequestParameters( RequestParameterType.SIZE );
    }


    @Override protected ServletResponseStrategy handleRequestInternal( final DS3Request request,
            final CommandExecutionParams params ) {
        final BeansServiceManager serviceManager = params.getServiceManager();
        final String bucketName = request.getBucketName();
        final UUID jobId = params.getRequest()
                .getRequestParameter(RequestParameterType.JOB)
                .getUuid();
        final UUID blobId = params.getRequest()
                .getRequestParameter(RequestParameterType.BLOB)
                .getUuid();

        serviceManager.getService(BucketService.class)
                .attain(Bucket.NAME, bucketName);
        final JobRM jobRM = new JobRM(jobId, params.getServiceManager());
        final BlobRM blobRM = new BlobRM(blobId, params.getServiceManager());
        final S3ObjectRM objectRM = blobRM.getObject();
        final BucketRM bucketRM = jobRM.getBucket();
        final DataPolicy dataPolicy = bucketRM.getDataPolicy()
                .unwrap();

        LOG.info("Processing object '" + objectRM.getName() + "' in bucket '" + bucketRM.getName() + "'.");

        final List<JobEntry> entries = jobRM.getJobEntries().toList();
        if (entries.isEmpty()) {
            throw new S3RestException(GenericFailure.BAD_REQUEST, "Job " + jobId + " has no job entries.");
        }

        if (!entries.stream().anyMatch(entry -> entry.getBlobId().equals(blobId))) {
            throw new S3RestException(GenericFailure.NOT_FOUND,
                    "Blob id " + blobId + " not found in job entries from job " + jobId);
        }

        if (jobRM.getRequestType() == JobRequestType.GET) {
            final BlobCache bc = serviceManager.getService( BlobCacheService.class ).retrieveByBlobId( blobId );
            if (bc != null && bc.getState() == CacheEntryState.IN_CACHE) {
                params.getPlannerResource().blobReadCompleted( jobId, blobId );
            } else if (entries.size() > 1) {
                throw new S3RestException(GenericFailure.BAD_REQUEST, "Blob " + blobId + " cannot be marked complete because it is not yet in cache, and job " + jobId + " cannot be cancelled because it has multiple entries.");
            } else {
                params.getTargetResource().cancelJob( params.getRequest().getAuthorization().getUserId(), jobId, false );
            }
        } else if (jobRM.getRequestType() == JobRequestType.PUT) {
            final long updatedSize;
            if (!request.hasRequestParameter(RequestParameterType.SIZE)) {
                updatedSize = -1;
            } else {
                updatedSize = params.getRequest()
                        .getRequestParameter(RequestParameterType.SIZE)
                        .getLong();
                if (updatedSize > blobRM.getLength()) {
                    throw new S3RestException(GenericFailure.BAD_REQUEST, "Completed blob size value in bytes must be " +
                            "less than or equal to the original size of the blob");
                } else if (updatedSize < 0) {
                    throw new S3RestException(GenericFailure.BAD_REQUEST, "Completed blob size value in bytes must be " +
                            "greater than or equal to 0");
                }
            }

            if (updatedSize >= 0) {
                final Blob blob = serviceManager.getService(BlobService.class).attain(blobId);
                final Job job = serviceManager.getService(JobService.class).attain(jobId);
                final BeansServiceManager transaction = serviceManager.startTransaction();
                transaction.getService(BlobService.class).update(
                        blob.setLength(updatedSize),
                        Blob.LENGTH);
                transaction.getService(JobService.class).update(
                        job.setOriginalSizeInBytes(updatedSize),
                        Job.ORIGINAL_SIZE_IN_BYTES);
                transaction.commitTransaction();
            }
            final ChecksumType clientChecksumType = ChecksumType.fromHttpRequest(request.getHttpRequest());
            if ((null == clientChecksumType) || (clientChecksumType != dataPolicy.getChecksumType())) {
                String checksumTypeString = clientChecksumType == null ? "<none>" : clientChecksumType.getAlgorithmName();
                throw new S3RestException(GenericFailure.BAD_REQUEST, "CRCs for this bucket must be of type " +
                        dataPolicy.getChecksumType()
                                .getAlgorithmName() + ".  You transmitted a " + checksumTypeString + ".");
            }
            final String checksum = request.getHttpRequest()
                    .getHeader(clientChecksumType);

            final List<S3ObjectProperty> objectProperties =
                    S3Utils.buildObjectPropertiesFromAmzCustomMetadataHeaders(request.getHttpRequest());

            //noinspection Duplicates
            if (null != blobRM.getChecksumType()) {
                switch (dataPolicy.getVersioning()) {
                    case NONE:
                        throw new S3RestException(GenericFailure.CONFLICT,
                                "Blob " + blobId + " for object " + objectRM.getId() + " has already been put.");
                    case KEEP_LATEST:
                        throw new S3RestException(GenericFailure.CONFLICT,
                                "Blob " + blobId + " for object " + objectRM.getId() + " has already been put." +
                                        "  If you desire to upload a newer version of this object, " +
                                        "you must either (i) wait for the current version to be entirely persisted" +
                                        ", or (ii) create a PUT job so that it's clear you intend to create" +
                                        " a newer version rather than are attempting to upload the already existing" +
                                        " object again.");
                    case KEEP_MULTIPLE_VERSIONS:
                        break;
                    default:
                        throw new UnsupportedOperationException("No code to support: " + dataPolicy.getVersioning());
                }
            }

            final String objectCreationDateHeader = request.getHttpRequest()
                    .getHeader(
                            S3HeaderType.OBJECT_CREATION_DATE.getHttpHeaderName());
            final Long objectCreationDate =
                    (null == objectCreationDateHeader) ? null : Long.parseLong(objectCreationDateHeader);

            final Boolean completionResult = params.getPlannerResource()
                    .blobWriteCompleted(jobRM.getId(), blobId, clientChecksumType, checksum,
                            objectCreationDate,
                            CollectionFactory.toArray(S3ObjectProperty.class,
                                    objectProperties))
                    .get(RpcFuture.Timeout.LONG);

            if (jobRM.isReplicating() && null != completionResult) {
                if (!completionResult) {
                    throw new S3RestException(GenericFailure.BAD_REQUEST,
                            "Replicating jobs must specify the " + S3HeaderType.OBJECT_CREATION_DATE.getHttpHeaderName() +
                                    " HTTP header.");
                }
            }
        } else {
            throw new S3RestException(GenericFailure.BAD_REQUEST, "Unsupported job request type: " + jobRM.getRequestType());
        }

        params.getServiceManager()
              .getService( S3ObjectPropertyService.class )
              .populateAllHttpHeaders( blobId, request.getHttpResponse() );

        return BeanServlet.serviceRequest( params, HttpServletResponse.SC_OK, null );
    }
}
