/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.*;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.server.domain.JobWithChunksApiBean;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared.JobResponseBuilder;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class GetJobChunksReadyForClientProcessingRequestHandler extends BaseRequestHandler
{
    public GetJobChunksReadyForClientProcessingRequestHandler()
    {
        super( new BucketAuthorizationStrategy( 
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.JOB, 
                AdministratorOverride.NO ),
               new RestfulCanHandleRequestDeterminer( 
                RestActionType.LIST, 
                RestDomainType.JOB_CHUNK ) );
        
        registerRequiredRequestParameters( 
                RequestParameterType.JOB );
        registerOptionalRequestParameters(
                RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS,
                RequestParameterType.JOB_CHUNK );
    }
    

    @Override
    synchronized protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        params.getPlannerResource().cleanUpCompletedJobsAndJobChunks().get( Timeout.DEFAULT );
        final int preferredNumberOfChunks = 
                ( request.hasRequestParameter( RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS ) ) ?
                      request.getRequestParameter( RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS ).getInt()
                      : s_defaultPreferredNumberOfChunks;
        final BeansRetrieverManager brm = params.getServiceManager();
        final Job job = brm.getRetriever( Job.class ).attain(
                request.getRequestParameter( RequestParameterType.JOB ).getUuid() );
        params.getPlannerResource().jobStillActive( job.getId(), null );
        
        Set<DetailedJobEntry> readyEntries =  BeanUtils.sort( getEntries( job, brm ) );
        LOG.info( readyEntries.size() + " entries will be considered for being ready for client processing." );
        final DataPathBackend backend = params.getServiceManager().getRetriever( DataPathBackend.class ).attain(
                Require.nothing() );
        final UUID requestedEntryId =
                ( request.hasRequestParameter( RequestParameterType.JOB_CHUNK ) ) ?
                        request.getRequestParameter( RequestParameterType.JOB_CHUNK ).getUuid()
                        : null;
        if ( requestedEntryId != null ) {
            if (backend.getEmulateChunks()) {
                readyEntries.removeIf( entry -> !entry.getChunkId().equals( requestedEntryId ) );
            } else {
                readyEntries.removeIf( entry -> !entry.getId().equals( requestedEntryId ) );
            }
        }

        if ( readyEntries.isEmpty() )
        {
            if ( JobRequestType.PUT == job.getRequestType()
                    || 0 == brm.getRetriever( JobEntry.class ).getCount( JobEntry.JOB_ID, job.getId() ) )
            {
                return BeanServlet.serviceRequest(
                        params,
                        HttpServletResponse.SC_GONE,
                        null );
            }
        }

        Long chunkSize = params.getServiceManager().getRetriever(Bucket.class).attain(job.getBucketId()).getLastPreferredChunkSizeInBytes();
        if (chunkSize == null) {
            chunkSize = BucketService.DEFAULT_PREFFERRED_CHUNK_SIZE;
        }
        final long preferredTotalSizeInBytes = preferredNumberOfChunks * chunkSize;

        Map<UUID, List<DetailedJobEntry> > emulatedChunks = new LinkedHashMap<>();
        switch ( job.getRequestType() )
        {
            case PUT:
                if (backend.getEmulateChunks()) {
                    emulatedChunks = allocateToPreferredNumberOfChunks(
                            params.getPlannerResource(),
                            readyEntries,
                            preferredNumberOfChunks
                    );
                } else {
                    readyEntries = allocateToPreferredSizeInBytes(
                            params.getPlannerResource(),
                            readyEntries,
                            preferredTotalSizeInBytes);
                }
                break;
            case GET:
                handleGetChunks(params, readyEntries, preferredTotalSizeInBytes, job);
                break;
            default:
                throw new UnsupportedOperationException( "No code for " + job.getRequestType() + "." );
        }

        final JobResponseBuilder repsonseBuilder = new JobResponseBuilder(
                job.getId(),
                params);
        final JobWithChunksApiBean response;
        // Chunk emulation only applies to PUTs; GETs are always reported one
        // entry per chunk so we don't delay telling the client a file is ready.
        if (backend.getEmulateChunks() && JobRequestType.PUT == job.getRequestType()) {
            for ( final UUID chunkId : emulatedChunks.keySet() ) {
                repsonseBuilder.addChunk(chunkId, emulatedChunks.get(chunkId));
            }
            response = repsonseBuilder.buildFromEmulatedChunks();
        } else {
            response = repsonseBuilder.build(readyEntries);
        }
        params.getRequest().getHttpResponse().addHeader(
                "Retry-After",
                Integer.toString( backend.getCacheAvailableRetryAfterInSeconds() ) );
        return BeanServlet.serviceGet( params, response );
    }

    private static void handleGetChunks(CommandExecutionParams params, Set<DetailedJobEntry> readyEntries, long preferredTotalSizeInBytes, Job job) {
        final List<DetailedJobEntry> entries = new ArrayList<>(readyEntries);
        long totalSize = entries.stream().mapToLong( DetailedJobEntry::getLength ).sum();
        while ( totalSize > preferredTotalSizeInBytes)
        {
            final DetailedJobEntry entry = entries.remove( entries.size() - 1 );
            readyEntries.remove( entry );
            totalSize -= entry.getLength();
        }

        if ( JobChunkClientProcessingOrderGuarantee.IN_ORDER
                == job.getChunkClientProcessingOrderGuarantee() )
        {
            final List<JobEntry> allEntries = new ArrayList<>( BeanUtils.sort(
                    params.getServiceManager().getRetriever( JobEntry.class ).retrieveAll(
                            JobEntry.JOB_ID, job.getId() ).toSet() ) );
            final long nextChunkNumber = allEntries.get( 0 ).getChunkNumber();
            if ( !entries.isEmpty() && nextChunkNumber != entries.get( 0 ).getChunkNumber() )
            {
                entries.clear();
            }

            readyEntries.clear();
            for ( int i = 0; i < allEntries.size(); ++i )
            {
                final UUID readyChunkId = ( i >= entries.size() ) ? null : entries.get( i ).getId();
                if ( allEntries.get( i ).getId().equals( readyChunkId ) )
                {
                    readyEntries.add( entries.get( i ) );
                }
                else
                {
                    break;
                }
            }
        }
    }


    private static Set<DetailedJobEntry> getEntries(final Job job, final BeansRetrieverManager brm )
    {
        switch ( job.getRequestType() )
        {
            case PUT:
                return getEntriesForPutJob( job, brm );
            case GET:
                return getEntriesForGetJob( job, brm );
            default: throw new UnsupportedOperationException( "No code for: " + job.getRequestType() );
        }
    }

    private static Set<DetailedJobEntry> getEntriesForPutJob(final Job job, final BeansRetrieverManager brm )
    {
        return brm.getRetriever( DetailedJobEntry.class ).retrieveAll( Require.all(
                Require.beanPropertyEquals( JobEntry.JOB_ID, job.getId() ),
                Require.any(
                        Require.beanPropertyNull(DetailedJobEntry.CACHE_STATE),
                        Require.all(
                                Require.beanPropertyNotNull(DetailedJobEntry.CACHE_STATE),
                                Require.beanPropertyEquals(DetailedJobEntry.CACHE_STATE, CacheEntryState.ALLOCATED)
                        )
                ),
                Require.beanPropertyEquals(
                        JobEntry.BLOB_STORE_STATE,
                        JobChunkBlobStoreState.PENDING) ) ).toSet();
    }


    private static Set<DetailedJobEntry> getEntriesForGetJob(final Job job, final BeansRetrieverManager brm )
    {
        return brm.getRetriever( DetailedJobEntry.class ).retrieveAll( Require.all(
                Require.beanPropertyEquals( JobEntry.JOB_ID, job.getId() ),
                Require.beanPropertyEquals(
                        JobEntry.BLOB_STORE_STATE,
                        JobChunkBlobStoreState.COMPLETED ) ) ).toSet();
    }


    private Set<DetailedJobEntry> allocateToPreferredSizeInBytes(
            final DataPlannerResource dataPlannerResource,
            final Set<DetailedJobEntry> readyChunks,
            final long preferredSizeInBytes )
    {
        final List<DetailedJobEntry> candidates = new ArrayList<>();
        long candidateBytes = 0;
        for ( final DetailedJobEntry entry : readyChunks )
        {
            if ( candidateBytes >= preferredSizeInBytes )
            {
                break;
            }
            candidates.add( entry );
            candidateBytes += entry.getLength();
        }

        final UUID[] candidateIds = candidates.stream()
                .map( DetailedJobEntry::getId )
                .toArray( UUID[]::new );
        final Set<UUID> allocatedIds = new HashSet<>( Arrays.asList( dataPlannerResource
                        .allocateEntries(true, candidateIds).get( Timeout.LONG ).getEntriesInCache() ) );

        final Set<DetailedJobEntry> allocatedChunks = new LinkedHashSet<>();
        for ( final DetailedJobEntry entry : candidates )
        {
            if ( !allocatedIds.contains( entry.getId() ) )
            {
                break;
            }
            allocatedChunks.add( entry );
        }
        return allocatedChunks;
    }


    private Map<UUID, List<DetailedJobEntry> > allocateToPreferredNumberOfChunks(
            final DataPlannerResource dataPlannerResource,
            final Set<DetailedJobEntry> readyChunks,
            final int preferredNumberOfChunks )
    {
        // Group entries by emulated chunk ID, preserving sort order, up to the preferred limit.
        final Map<UUID, List<DetailedJobEntry>> candidateChunks = new LinkedHashMap<>();
        UUID curChunkId = null;
        for ( final DetailedJobEntry entry : readyChunks )
        {
            if ( !entry.getChunkId().equals( curChunkId ) )
            {
                if ( candidateChunks.size() >= preferredNumberOfChunks )
                {
                    break;
                }
                if ( candidateChunks.containsKey( entry.getChunkId() ) )
                {
                    LOG.warn( "Non-contiguous chunk IDs detected when allocating to preferred number of chunks." );
                }
                else
                {
                    candidateChunks.put( entry.getChunkId(), new ArrayList<>() );
                }
            }
            curChunkId = entry.getChunkId();
            candidateChunks.get( curChunkId ).add( entry );
        }

        // Attempt atomic allocation per emulated chunk; stop at the first failure.
        final Map<UUID, List<DetailedJobEntry>> allocatedChunks = new LinkedHashMap<>();
        for ( final Map.Entry<UUID, List<DetailedJobEntry>> chunk : candidateChunks.entrySet() )
        {
            final List<DetailedJobEntry> chunkEntries = chunk.getValue();
            final UUID[] entryIds = chunkEntries.stream()
                    .map( DetailedJobEntry::getId )
                    .toArray( UUID[]::new );
            final UUID[] allocated = dataPlannerResource
                    .allocateEntries(false, entryIds).get( Timeout.LONG ).getEntriesInCache();
            if ( allocated.length < entryIds.length )
            {
                //NOTE: this line should be unreachable since we did now allow partial
                LOG.info( "Cannot atomically allocate emulated chunk " + chunk.getKey() + "." );
                break;
            }
            allocatedChunks.put( chunk.getKey(), chunkEntries );
        }
        return allocatedChunks;
    }


    static int getDefaultPreferredNumberOfChunks()
    {
        return s_defaultPreferredNumberOfChunks;
    }
    
    
    static void setDefaultPreferredNumberOfChunks( final int value )
    {
        Validations.verifyInRange( "Default preferred number of chunks", 2, 1024, value );
        s_defaultPreferredNumberOfChunks = value;
    }
    
    
    private static volatile int s_defaultPreferredNumberOfChunks = 6;
}
