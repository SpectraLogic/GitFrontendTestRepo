/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared;

import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import lombok.val;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.platform.domain.BlobApiBeanBuilder;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.server.domain.JobChunkApiBean;
import com.spectralogic.s3.server.domain.JobStatus;
import com.spectralogic.s3.server.domain.JobWithChunksApiBean;
import com.spectralogic.s3.server.domain.NodeApiBean;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class JobResponseBuilder
{
    public JobResponseBuilder(
            final UUID jobId,
            final CommandExecutionParams params )
    {
        m_jobId = jobId;
        m_brm = params.getServiceManager();
        m_dataPlannerResource = params.getPlannerResource();
        m_hostAddress = params.getRequest().getHttpRequest().getHeader("host");
        Validations.verifyNotNull( "Job id", m_jobId );
        Validations.verifyNotNull( "Retriever manager", m_brm );
        Validations.verifyNotNull( "Data Planner Resource", m_dataPlannerResource );
    }
    

    public JobWithChunksApiBean buildFromDatabase()
    {
        LOG.info( "Building " + JobWithChunksApiBean.class.getSimpleName() 
                  + " response for job " + m_jobId + "..." );

        final Job job = m_brm.getRetriever( Job.class ).attain( m_jobId );
        final Set<JobEntry> daoChunks = m_brm.getRetriever( JobEntry.class ).retrieveAll(
                JobEntry.JOB_ID,
                job.getId() ).toSet();
        return build( daoChunks );
    }

    public JobResponseBuilder addChunk(final UUID chunkId, final List<? extends JobEntry> entries) {
        m_emulatedChunks.put(chunkId, entries);
        return this;
    }

    public JobWithChunksApiBean buildFromEmulatedChunks()
    {

        final JobWithChunksApiBean retval = initializeResponse( Job.class );
        if ( JobRequestType.VERIFY == retval.getRequestType() )
        {
            retval.setEntirelyInCache( Boolean.FALSE );
        }
        else
        {
            boolean entirelyInCache = true;
            for ( final JobEntry chunk : m_emulatedChunks.values().stream().flatMap(List::stream).toList() )
            {
                entirelyInCache = checkIfEntryInCache(chunk) && entirelyInCache;
            }
            retval.setEntirelyInCache( Boolean.valueOf( entirelyInCache ) );
        }
        retval.setNodes( getNodesPayload() );
        if ( m_emulatedChunks.isEmpty() )
        {
            return retval;
        }

        final Set< Blob > blobs =
                m_brm.getRetriever( Blob.class ).retrieveAll(
                        Require.exists(
                                JobEntry.class,
                                BlobObservable.BLOB_ID,
                                Require.beanPropertyEquals( JobEntry.JOB_ID, m_jobId ) ) ).toSet();

        final Map< UUID, Blob > blobsMap = BeanUtils.toMap( blobs );
        final Set< UUID > blobIdsInCache = getBlobsInCache( blobs );
        final List< JobChunkApiBean > chunks = new ArrayList<>();
        int chunkNumber = 0;
        for ( final UUID chunkId : m_emulatedChunks.keySet())
        {
            chunks.add( buildChunk( chunkId, chunkNumber++, Set.copyOf(
                    m_emulatedChunks.get(chunkId).stream()
                            .map(je -> blobsMap.get(je.getBlobId()))
                            .toList()), blobIdsInCache ) );
        }
        retval.setObjects( CollectionFactory.toArray( JobChunkApiBean.class, chunks ) );

        return retval;
    }


    public JobWithChunksApiBean build( Set<? extends JobEntry> daoChunks )
    {
        daoChunks = BeanUtils.sort( daoChunks );

        final JobWithChunksApiBean retval = initializeResponse( Job.class );
        if ( JobRequestType.VERIFY == retval.getRequestType() )
        {
            retval.setEntirelyInCache( Boolean.FALSE );
        }
        else
        {
            boolean entirelyInCache = true;
            for ( final JobEntry chunk : daoChunks )
            {
                entirelyInCache = checkIfEntryInCache(chunk) && entirelyInCache;
            }
            retval.setEntirelyInCache( Boolean.valueOf( entirelyInCache ) );
        }
        retval.setNodes( getNodesPayload() );
        if ( daoChunks.isEmpty() )
        {
            return retval;
        }
        
        final Set< Blob > blobs =
                m_brm.getRetriever( Blob.class ).retrieveAll(
                        Require.exists(
                                JobEntry.class,
                                BlobObservable.BLOB_ID, 
                                Require.beanPropertyEquals( JobEntry.JOB_ID, m_jobId ) ) ).toSet();

        final Map< UUID, Blob > blobsMap = BeanUtils.toMap( blobs );
        final Set< UUID > blobIdsInCache = getBlobsInCache( blobs );
        final List< JobChunkApiBean > chunks = new ArrayList<>();
        for ( final JobEntry daoChunk : daoChunks )
        {
            chunks.add( buildChunk( daoChunk, Set.of(blobsMap.get(daoChunk.getBlobId())), blobIdsInCache ) );
        }
        retval.setObjects( CollectionFactory.toArray( JobChunkApiBean.class, chunks ) );
        
        return retval;
    }
    
    private boolean checkIfEntryInCache(final JobEntry entry) {
        BlobCache bc =  m_brm.getRetriever(BlobCache.class).retrieve( BlobCache.BLOB_ID, entry.getBlobId());
        if ( bc == null || bc.getState() != CacheEntryState.IN_CACHE ) {
           return false;
        }
        return true;
    }
    public < T extends Identifiable & JobObservable< ? > > JobWithChunksApiBean initializeResponse(
            final Class< T > jobDaoType )
    {
        final T job = m_brm.getRetriever( jobDaoType ).attain( m_jobId );
        return initializeResponse( job );
    }
    
    
    public < T extends Identifiable & JobObservable< ? > > JobWithChunksApiBean initializeResponse(
            final T job )
    {
        final JobWithChunksApiBean retval = BeanFactory.newBean( JobWithChunksApiBean.class );
        BeanCopier.copy( retval, job );
        retval.setStatus( JobStatus.IN_PROGRESS );
        retval.setBucketName( m_brm.getRetriever( Bucket.class ).attain( job.getBucketId() ).getName() );
        retval.setStartDate( job.getCreatedAt() );
        retval.setJobId( job.getId() );
        retval.setUserName( m_brm.getRetriever( User.class ).attain( job.getUserId() ).getName() );

        return retval;
    }
    
    
    /**
     * @param blobIdsInCache - optional, provide to improve performance by eliminating the need to retrieve
     * the blobs in cache
     */
    public JobChunkApiBean buildChunk(
            final JobEntry daoChunk,
            final Set< Blob > blobs,
            Set< UUID > blobIdsInCache )
    {
        final JobChunkApiBean chunk = BeanFactory.newBean( JobChunkApiBean.class );
        chunk.setChunkId( daoChunk.getId() );
        chunk.setChunkNumber( daoChunk.getChunkNumber() );
        
        if ( null == blobIdsInCache )
        {
            blobIdsInCache = getBlobsInCache( blobs );
        }
        chunk.setObjects( new BlobApiBeanBuilder( null, m_brm.getRetriever( S3Object.class ), blobs )
                .includeBlobCacheState( blobIdsInCache ).build() );
        return chunk;
    }


    private JobChunkApiBean buildChunk(
            final UUID chunkId,
            final int chunkNumber,
            final Set< Blob > blobs,
            Set< UUID > blobIdsInCache )
    {
        final JobChunkApiBean chunk = BeanFactory.newBean( JobChunkApiBean.class );
        chunk.setChunkId( chunkId );
        chunk.setChunkNumber( chunkNumber );

        if ( null == blobIdsInCache )
        {
            blobIdsInCache = getBlobsInCache( blobs );
        }
        chunk.setObjects( new BlobApiBeanBuilder( null, m_brm.getRetriever( S3Object.class ), blobs )
                .includeBlobCacheState( blobIdsInCache ).build() );
        return chunk;
    }
    
    
    private Set< UUID > getBlobsInCache( final Collection< Blob > blobs )
    {
        final Set< UUID > retval = new HashSet<>();
        final Set< UUID > segment = new HashSet<>();
        for ( final Blob blob : blobs )
        {
            segment.add( blob.getId() );
            if ( 10000 == segment.size() )
            {
                retval.addAll( getBlobsInCacheInternal( segment ) );
                segment.clear();
            }
        }
        retval.addAll( getBlobsInCacheInternal( segment ) );
        return retval;
    }
    
    
    private Set< UUID > getBlobsInCacheInternal( final Set< UUID > blobIds )
    {
        if ( blobIds.isEmpty() )
        {
            return new HashSet<>();
        }
        
        return CollectionFactory.toSet( m_dataPlannerResource
                .getBlobsInCache( CollectionFactory.toArray(
                        UUID.class,
                        blobIds ) )
                .get( Timeout.LONG )
                .getBlobsInCache() );
    }


    private NodeApiBean [] getNodesPayload()
    {
        final List< NodeApiBean > retval = new ArrayList<>();
        
        for ( final Node node : m_brm.getRetriever( Node.class ).retrieveAll().toSet() )
        {
            node.setDataPathIpAddress(m_hostAddress);
            retval.add( (NodeApiBean)BeanFactory.newBean( NodeApiBean.class )
                    .setEndPoint( ( null == node.getDnsName() ) ?
                            node.getDataPathIpAddress()
                            : node.getDnsName() )
                    .setHttpPort( node.getDataPathHttpPort() )
                    .setHttpsPort( node.getDataPathHttpsPort() )
                    .setId( node.getId() ) );
        }
        
        return CollectionFactory.toArray( NodeApiBean.class, retval );
    }
    
    
    private final UUID m_jobId;
    private final BeansRetrieverManager m_brm;
    private final DataPlannerResource m_dataPlannerResource;
    private final String m_hostAddress;
    private final Map<UUID, List<? extends JobEntry>> m_emulatedChunks = new LinkedHashMap<>();

    private final static Logger LOG = Logger.getLogger( JobResponseBuilder.class );
}
