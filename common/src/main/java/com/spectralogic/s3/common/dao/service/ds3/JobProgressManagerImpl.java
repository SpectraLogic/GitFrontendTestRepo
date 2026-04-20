/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.S3ObjectsCachedNotificationPayloadGenerator;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

public final class JobProgressManagerImpl implements JobProgressManager
{
    public JobProgressManagerImpl( 
            final BeansServiceManager serviceManager )
    {
        this( serviceManager, BufferProgressUpdates.YES );
    }
    
    public JobProgressManagerImpl( 
            final BeansServiceManager serviceManager,
            final BufferProgressUpdates bufferProgressUpdates )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Buffering mode", bufferProgressUpdates );
        m_jobService = serviceManager.getService( JobService.class );
        m_bufferProgressUpdates = bufferProgressUpdates;
        if ( BufferProgressUpdates.YES == m_bufferProgressUpdates )
        {
            m_recurringRunnableExecutor.start();
        }
    }
    
    
    public enum BufferProgressUpdates
    {
        /** 
         * Progress updates should usually be buffered, which greatly improves performance.
         */
        YES,
        
        /** 
         * Progress updates may need to be committed immediately without buffering for unit tests, do NOT use this
         * option for production since it can degrade performance by an order of magnitude or more, and more
         * importantly is NOT threadsafe. Use with frequent updates may result in deadlock or missed updates.
         */
        @Deprecated
        NO
    }
    
    
    private final class PeriodicJobProgressFlusher implements Runnable
    {
        public void run()
        {
            flush();
        }
    } // end inner class def
    
    
    
    public void flush()
    {
        flush( m_jobService );
    }
    
    
    public void blobLoadedToCache( final UUID jobId, final long size )
    {
        Validations.verifyNotNull( "Job id", jobId );
        m_queuedBytesOfWorkCached.compute( jobId,
        		( k, v ) -> v == null ? size : v + size );
        if ( BufferProgressUpdates.NO == m_bufferProgressUpdates )
        {
        	flush();
        }
        		
    }
    
    
    public void entryLoadedToCache(final BeansServiceManager transaction, final JobEntry chunk )
    {
        final long bytesLoaded = transaction.getService( JobEntryService.class ).getSizeInBytes( chunk.getId() );
        bytesLoadedToCache(transaction, bytesLoaded, chunk.getJobId());
    }


    public void entriesLoadedToCache(final BeansServiceManager transaction, final Collection<JobEntry> entries)
    {
        final Map<UUID, Collection<JobEntry> > entriesByJobId = new HashMap<>();
        for (final JobEntry entry : entries) {
            entriesByJobId.computeIfAbsent(entry.getJobId(), k -> new ArrayList<>()).add(entry);
        }
        for (final Map.Entry<UUID, Collection<JobEntry>> entry : entriesByJobId.entrySet()) {
            final UUID jobId = entry.getKey();
            final Collection<JobEntry> entriesForJob = entry.getValue();
            final long bytesLoaded = transaction.getRetriever( Blob.class ).getSum(
                    Blob.LENGTH,
                    Require.beanPropertyEqualsOneOf(
                            Identifiable.ID,
                            BeanUtils.extractPropertyValues(entriesForJob, JobEntry.BLOB_ID)
                    )
            );
            bytesLoadedToCache(transaction, bytesLoaded, jobId);
            transaction.getNotificationEventDispatcher().fire(new JobNotificationEvent(
                    transaction.getRetriever(Job.class).attain(jobId),
                    transaction.getRetriever(S3ObjectCachedNotificationRegistration.class),
                    new S3ObjectsCachedNotificationPayloadGenerator(
                            jobId,
                            entriesByJobId.get(jobId),
                            transaction.getRetriever(S3Object.class),
                            transaction.getRetriever(Blob.class))));
        }


    }


    public void bytesLoadedToCache( final BeansServiceManager transaction, final long bytesLoaded, final UUID jobId )
    {
        m_queuedBytesOfWorkCached.compute( jobId,
                ( k, v ) -> v == null ? bytesLoaded : v + bytesLoaded );
        if ( BufferProgressUpdates.NO == m_bufferProgressUpdates )
        {
            flush( transaction.getService( JobService.class ) );
        }
    }
    
    
    private void flush( final JobService jobService )
    {
        final MonitoredWork work = new MonitoredWork( StackTraceLogging.SHORT, "Flush job progress information" );
        try
        {
            flushJobProgressUpdates( jobService );
        }
        finally
        {
            work.completed();
        }
    }
    
    
    private void flushJobProgressUpdates( final JobService jobService )
    {
        final Set< UUID > jobIds = new HashSet<>( m_queuedBytesOfWorkCompleted.keySet() );
        jobIds.addAll( m_queuedBytesOfWorkCached.keySet() );
        final Set< UUID > removedJobs = new HashSet<>();
        for ( final UUID jobId : jobIds )
        {
    		final Job job = jobService.retrieve( jobId );
    		if ( null == job )
    		{
    			removedJobs.add( jobId );
    		}
    		else
    		{
	    		final Long cached = m_queuedBytesOfWorkCached.remove( jobId );
	    		final Long completed = m_queuedBytesOfWorkCompleted.remove( jobId );
	            job.setCachedSizeInBytes( job.getCachedSizeInBytes() +
	                    ( ( null == cached ) ? 0 : cached.longValue() ) );
	            job.setCompletedSizeInBytes( job.getCompletedSizeInBytes() +
	            		( ( null == completed ) ? 0 : completed.longValue() ) );
	            if ( null != cached || null != completed )
	            {
		            jobService.update(
		                    job, JobObservable.COMPLETED_SIZE_IN_BYTES, JobObservable.CACHED_SIZE_IN_BYTES );
	            }
    		}
        }
        m_queuedBytesOfWorkCached.keySet().removeAll( removedJobs );
        m_queuedBytesOfWorkCompleted.keySet().removeAll( removedJobs );
    }
    
    
    public void workCompleted( final UUID jobId, final long bytesOfWorkCompleted )
    {
        m_queuedBytesOfWorkCompleted.compute( jobId,
        		( k, v ) -> v == null ? bytesOfWorkCompleted : v + bytesOfWorkCompleted );
        if ( BufferProgressUpdates.NO == m_bufferProgressUpdates )
        {
        	flush();
        }
    }
    
    
    private final Map< UUID, Long > m_queuedBytesOfWorkCached = new ConcurrentHashMap<>();
    private final Map< UUID, Long > m_queuedBytesOfWorkCompleted = new ConcurrentHashMap<>();
    private final JobService m_jobService;
    private final BufferProgressUpdates m_bufferProgressUpdates;
    
    private final RecurringRunnableExecutor m_recurringRunnableExecutor =
            new RecurringRunnableExecutor( new PeriodicJobProgressFlusher(), 5000 );
}
