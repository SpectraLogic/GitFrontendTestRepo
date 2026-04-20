/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BaseCreateJobParams;
import com.spectralogic.s3.dataplanner.cache.JobCreatedListener;
import com.spectralogic.s3.dataplanner.frontend.api.JobCreator;
import com.spectralogic.s3.dataplanner.frontend.api.JobEntryGrouping;
import com.spectralogic.s3.dataplanner.frontend.api.S3ObjectCreator;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public class MockJobCreator implements JobCreator
{
    public UUID createJob(
            final BaseCreateJobParams< ? > params,
            final UUID jobId,
            final JobRequestType requestType,
            final JobChunkClientProcessingOrderGuarantee chunkClientProcessingOrderGuarantee,
            final S3ObjectCreator objectCreator,
            final List<JobEntry> groupings )
    {
        m_createJobCallCount.incrementAndGet();
        return UUID.randomUUID();
    }
    
    
    public int getCreateJobCallCount()
    {
        return m_createJobCallCount.get();
    }
    
    
    public long getPreferredBlobSizeInBytes()
    {
        return 1024;
    }
    
    
    public long getPreferredChunkSizeInBytes()
    {
        return 1024;
    }


    @Override
    public UUID createGetOrVerifyJob(BaseCreateJobParams<?> params, UUID jobId, JobRequestType requestType, JobChunkClientProcessingOrderGuarantee chunkClientProcessingOrderGuarantee, List<JobEntry> jobEntries) {
        m_createJobCallCount.incrementAndGet();
        return UUID.randomUUID();
    }

    @Override
    public UUID createPutJob(BaseCreateJobParams<?> params, UUID jobId, S3ObjectCreator objectCreator, List<JobEntry> jobEntries) {
        m_createJobCallCount.incrementAndGet();
        return UUID.randomUUID();
    }

    public void addJobCreatedListener(final JobCreatedListener listener )
    {
        m_listeners.add( listener );
    }
    
    
    public void notifyJobCreatedListeners( final JobRequestType type, final UUID jobId )
    {
        for ( final JobCreatedListener listener : m_listeners )
        {
            listener.jobCreated( type, jobId );
        }
    }
    
    
    public void regenerateJobChunk(
            final Job job,
            final JobEntry chunk,
            final List< JobEntryGrouping > jobEntryGroupings, 
            final BeansServiceManager transaction,
            final Set< Blob > blobs,
            final boolean minimizeSpanningAcrossMedia )
    {
        // empty
    }
    
    
    public void regenerateJob(
            final Job job,
            final List< JobEntryGrouping > groupings,
            final BeansServiceManager transaction,
            final Set< Blob > blobs,
            final boolean minimizeSpanningAcrossMedia )
    {
        // empty
    }
    
    
    public Object getJobReshapingLock()
    {
        return m_jobReshapingLock;
    }
    
    
    private final AtomicInteger m_createJobCallCount = new AtomicInteger();
    private final List< JobCreatedListener > m_listeners = new CopyOnWriteArrayList<>();
    private final Object m_jobReshapingLock = new Object();
}
