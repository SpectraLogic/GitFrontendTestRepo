package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.orm.*;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.domain.*;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.render.BytesRenderer;

import java.util.*;

public class GetJobSummaryRequestHandler extends BaseRequestHandler {
    public GetJobSummaryRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                        SystemBucketAccess.STANDARD,
                        BucketAclPermission.JOB,
                        AdministratorOverride.YES ),
                new RestfulCanHandleRequestDeterminer(
                        RestActionType.SHOW,
                        RestDomainType.JOB ) );
        registerRequiredRequestParameters(
                RequestParameterType.SUMMARY);
    }


    @Override
    synchronized protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final UUID jobId = request.getRestRequest().getId(
                params.getServiceManager().getRetriever( Job.class ) );
        final CompletedJob completedJob = params.getServiceManager().getRetriever( CompletedJob.class ).retrieve( jobId );
        final CanceledJob canceledJob = params.getServiceManager().getRetriever( CanceledJob.class ).retrieve( jobId );
        final Job job = params.getServiceManager().getRetriever( Job.class ).retrieve( jobId );
        List<DestinationSummary> destSummaries = null;
        String summary;
        if ( null !=  completedJob )
        {
            summary = "Job " + jobId + " has completed and is no longer active.";
        }
        else if ( null != canceledJob )
        {
            summary = "Job " + jobId + " is no longer active.";
        }
        else if (null == job) {
            throw new S3RestException( GenericFailure.NOT_FOUND,
                    "Could not find job with ID " + jobId  );
        } else {
            final BytesRenderer bytesRenderer = new BytesRenderer();
            final List<DetailedJobEntry> chunks = params.getServiceManager().getRetriever( DetailedJobEntry.class )
                    .retrieveAll( DetailedJobEntry.JOB_ID, jobId ).toList();
            int chunksAllocated = 0;
            int chunksCompleted = 0;
            int chunksInProgress = 0;
            for (final DetailedJobEntry chunk : chunks) {
                if (chunk.getCacheState() == CacheEntryState.ALLOCATED) {
                    chunksAllocated++;
                }
                if (chunk.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED) {
                    chunksCompleted++;
                } else if (chunk.getBlobStoreState() == JobChunkBlobStoreState.IN_PROGRESS) {
                    chunksInProgress++;
                }
            }
            summary = bytesRenderer.render(job.getCompletedSizeInBytes()) + " out of " +
                    bytesRenderer.render(job.getOriginalSizeInBytes()) + " in job " + jobId + " have been completed so" +
                    " far.\n";
            if (job.getRequestType() == JobRequestType.GET) {
                summary += chunks.size() + " chunk(s) of work remain in total.\n" + chunksAllocated + " chunk(s) of work are allocated in cache.\n" +
                        chunksInProgress + " are currently being read into cache.\n" +  chunksCompleted + " are ready to be read from cache by the client.";
            } else if (job.getRequestType() == JobRequestType.PUT) {
                summary += (chunks.size() - chunksCompleted) + " chunk(s) of work remain in total.\n" + chunksAllocated + " chunk(s) of work are allocated in cache.\n" +
                        chunksInProgress + " of those are fully in cache.";
                final Map<UUID, Map<Boolean, List<JobChunkApiBean>>> chunksByTargetIdAndIfDone = new HashMap<>();
                final Map<UUID, String> targetNames = new HashMap<>();
                for (final JobEntry chunk : chunks) {
                    final JobEntryRM chunkRM = new JobEntryRM(chunk, params.getServiceManager());

                    chunkRM.getLocalBlobDestinations().toList().forEach((jcpt) -> {
                        final JobChunkApiBean chunkApiBean = BeanFactory.newBean(JobChunkApiBean.class);
                        chunkApiBean.setChunkId(chunkRM.getId());
                        chunkApiBean.setChunkNumber(chunkRM.getChunkNumber());
                        chunksByTargetIdAndIfDone.computeIfAbsent(jcpt.getStorageDomainId(), (k) -> new HashMap<>())
                                .computeIfAbsent(jcpt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED, (k) -> new ArrayList<>()).add(chunkApiBean);
                        targetNames.computeIfAbsent(jcpt.getStorageDomainId(), (k) -> {
                            final StorageDomainRM storageDomainRM = new StorageDomainRM(jcpt.getStorageDomainId(), params.getServiceManager());
                            return "Storage Domain: " + storageDomainRM.getName();
                        });
                    });

                    chunkRM.getDs3BlobDestinations().toList().forEach(jct -> {
                        final JobChunkApiBean chunkApiBean = BeanFactory.newBean(JobChunkApiBean.class);
                        chunkApiBean.setChunkId(chunkRM.getId());
                        chunkApiBean.setChunkNumber(chunkRM.getChunkNumber());
                        chunksByTargetIdAndIfDone.computeIfAbsent(jct.getTargetId(), (k) -> new HashMap<>())
                                .computeIfAbsent(jct.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED, (k) -> new ArrayList<>()).add(chunkApiBean);
                        targetNames.computeIfAbsent(jct.getTargetId(), (k) -> {
                            final Ds3TargetRM targetRM = new Ds3TargetRM(jct.getTargetId(), params.getServiceManager());
                            return "DS3 Target: " + targetRM.getName();
                        });
                    });

                    chunkRM.getAzureBlobDestinations().toList().forEach(jct -> {
                        final JobChunkApiBean chunkApiBean = BeanFactory.newBean(JobChunkApiBean.class);
                        chunkApiBean.setChunkId(chunkRM.getId());
                        chunkApiBean.setChunkNumber(chunkRM.getChunkNumber());
                        chunksByTargetIdAndIfDone.computeIfAbsent(jct.getTargetId(), (k) -> new HashMap<>())
                                .computeIfAbsent(jct.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED, (k) -> new ArrayList<>()).add(chunkApiBean);
                        targetNames.computeIfAbsent(jct.getTargetId(), (k) -> {
                            final AzureTargetRM targetRM = new AzureTargetRM(jct.getTargetId(), params.getServiceManager());
                            return "Azure Target: " + targetRM.getName();
                        });
                    });

                    chunkRM.getS3BlobDestinations().toList().forEach(jct -> {
                        final JobChunkApiBean chunkApiBean = BeanFactory.newBean(JobChunkApiBean.class);
                        chunkApiBean.setChunkId(chunkRM.getId());
                        chunkApiBean.setChunkNumber(chunkRM.getChunkNumber());
                        chunksByTargetIdAndIfDone.computeIfAbsent(jct.getTargetId(), (k) -> new HashMap<>())
                                .computeIfAbsent(jct.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED, (k) -> new ArrayList<>()).add(chunkApiBean);
                        targetNames.computeIfAbsent(jct.getTargetId(), (k) -> {
                            final S3TargetRM targetRM = new S3TargetRM(jct.getTargetId(), params.getServiceManager());
                            return "S3 Target: " + targetRM.getName();
                        });
                    });
                }
                final List<DestinationSummary> tempSummaries = new ArrayList<>();
                chunksByTargetIdAndIfDone.forEach((targetId, doneMap) -> {
                    final DestinationSummary destSummary = BeanFactory.newBean(DestinationSummary.class);
                    destSummary.setId(targetId);
                    destSummary.setName(targetNames.getOrDefault(targetId, "Unknown"));
                    destSummary.setCompletedChunks(CollectionFactory.toArray(JobChunkApiBean.class, doneMap.getOrDefault(true, new ArrayList<>())));
                    destSummary.setIncompleteChunks(CollectionFactory.toArray(JobChunkApiBean.class, doneMap.getOrDefault(false, new ArrayList<>())));
                    tempSummaries.add(destSummary);
                });

                destSummaries = tempSummaries;
            } else {
                throw new S3RestException( GenericFailure.NOT_FOUND,
                        "Summary not supported for job with type " + job.getRequestType() );
            }
        }

        final JobSummaryApiBean retval = BeanFactory.newBean( JobSummaryApiBean.class );
        retval.setSummary( summary );
        retval.setAggregating(job.isAggregating() );
        retval.setOriginalSizeInBytes( job.getOriginalSizeInBytes() );
        retval.setCachedSizeInBytes( job.getCachedSizeInBytes() );
        retval.setCompletedSizeInBytes( job.getCompletedSizeInBytes() );
        retval.setNaked( job.isNaked() );
        retval.setDestinationSummaries( destSummaries != null ? CollectionFactory.toArray( DestinationSummary.class, destSummaries ) : null );

        return BeanServlet.serviceGet(
                params,
                retval );
    }
}
