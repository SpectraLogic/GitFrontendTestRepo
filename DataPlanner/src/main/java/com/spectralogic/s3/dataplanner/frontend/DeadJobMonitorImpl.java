/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.ThrottledRunnable;
import com.spectralogic.util.thread.ThrottledRunnableExecutor;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

public final class DeadJobMonitorImpl implements DeadJobMonitor
{
    public DeadJobMonitorImpl(
            final int workQueueSize,
            final int millisSinceLastActivityToConsiderJobDead,
            final BeansRetrieverManager brm,
            final Ds3ConnectionFactory ds3ConnectionFactory )
    {
        this( workQueueSize, millisSinceLastActivityToConsiderJobDead, brm, ds3ConnectionFactory, 60000 );
    }
    
    
    DeadJobMonitorImpl(
            final int workQueueSize,
            final int millisSinceLastActivityToConsiderJobDead,
            final BeansRetrieverManager brm,
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final int delayToKeepJobsAliveOnTargets )
    {
        m_activitiesOccurred = ( 0 >= workQueueSize ) ? 
                null 
                : new ArrayBlockingQueue< ActivitySpecification >( workQueueSize );
        m_millisSinceLastActivityToConsiderJobDead = millisSinceLastActivityToConsiderJobDead;
        m_brm = brm;
        m_ds3ConnectionFactory = ds3ConnectionFactory;
        m_keepJobAliveOnTargetsExecutor = new ThrottledRunnableExecutor<>(
                delayToKeepJobsAliveOnTargets,
                new KeepJobAliveOnTargetsAggregator() );
        
        Validations.verifyNotNull( "Bean retriever manager", m_brm );
        Validations.verifyNotNull( "DS3 connection factory", m_ds3ConnectionFactory );
        
        if ( null != m_activitiesOccurred )
        {
            m_activityOccurredProcessorExecutor.start();
        }
    }   
    
    
    synchronized public boolean isDead( final UUID jobId )
    {
        if ( !m_jobs.containsKey( jobId ) )
        {
            reloadCurrentJobs();
        }
        
        final Duration durationSinceLastActivity = m_jobs.get( jobId );
        if ( null == durationSinceLastActivity )
        {
            // It's been deleted, so it's not dead needing to be killed
            return false;
        }
        
        return ( durationSinceLastActivity.getElapsedMillis() > 
                 m_millisSinceLastActivityToConsiderJobDead );
    }
    
    
    private void reloadCurrentJobs()
    {
        final Set< UUID > jobIds = new HashSet<>();
        for ( final Job job : m_brm.getRetriever( Job.class ).retrieveAll().toSet() )
        {
            jobIds.add( job.getId() );
            if ( !m_jobs.containsKey( job.getId() ) )
            {
                m_jobs.put( job.getId(), new Duration() );
            }
        }
        
        for ( final UUID id : new HashSet<>( m_jobs.keySet() ) )
        {
            if ( !jobIds.contains( id ) )
            {
                m_jobs.remove( id );
            }
        }
    }


    public void activityOccurred( final UUID jobId, final UUID blobId )
    {
        final ActivitySpecification as = new ActivitySpecification( jobId, blobId ); 
        if ( null == m_activitiesOccurred || null != jobId )
        {
            activityOccurred( as );
            return;
        }
        if ( m_activitiesOccurred.offer( as ) )
        {
            return;
        }
        
        LOG.info( "Call to activityOccurred must block and wait until the queue of events isn't full." );
        try
        {
            m_activitiesOccurred.put( as );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private void activityOccurred( final ActivitySpecification as )
    {
        final AtomicBoolean reloadOccurred = new AtomicBoolean( false );
        if ( null != as.m_jobId )
        {
            activityOccurredInternal( as.m_jobId, reloadOccurred );
            return;
        }
        
        final Set<JobEntry> jobEntries = m_brm.getRetriever( JobEntry.class ).retrieveAll(
                Require.beanPropertyEquals( BlobObservable.BLOB_ID, as.m_blobId ) ).toSet();
        for ( final JobEntry entry: jobEntries )
        {
            activityOccurredInternal( entry.getJobId(), reloadOccurred );
        }
        if ( 1 != jobEntries.size() )
        {
            LOG.info( "A non-optimal call was made that activity has occurred.  Activity occurred on blob "
                      + as.m_blobId 
                      + "; however, there were " 
                      + jobEntries.size() 
                      + " jobs that this activity could have been for " 
                      + "and the client didn't specify which one it was." );
        }
    }
    
    
    synchronized private void activityOccurredInternal( final UUID jobId, final AtomicBoolean reloadOccurred )
    {
        if ( !m_jobs.containsKey( jobId ) && !reloadOccurred.getAndSet( true ) )
        {
            reloadCurrentJobs();
        }
        if ( !m_jobs.containsKey( jobId ) )
        {
            // It's been deleted, so no need to record activity
            return;
        }

        LOG.debug( "Activity occurred for job " + jobId + "." );
        m_jobs.put( jobId, new Duration() );
        if ( !m_durationSinceJobKeptAliveOnTargets.containsKey( jobId ) 
                || m_millisSinceLastActivityToConsiderJobDead / 100 
                   < m_durationSinceJobKeptAliveOnTargets.get( jobId ).getElapsedMillis() )
        {
            m_durationSinceJobKeptAliveOnTargets.put( jobId, new Duration() );
            m_keepJobAliveOnTargetsExecutor.add(
                    new KeepJobAliveOnTargetsWorker().addJobIds( CollectionFactory.toSet( jobId ) ) );
        }
    }
    
    
    private final class KeepJobAliveOnTargetsWorker implements ThrottledRunnable
    {
        synchronized private KeepJobAliveOnTargetsWorker addJobIds( final Set< UUID > jobIds )
        {
            m_jobIds.addAll( jobIds );
            return this;
        }
        
        
        synchronized public void run( final RunnableCompletionNotifier completionNotifier )
        {
            LOG.info( "Will keep jobs " + m_jobIds + " alive on targets." );
            final MonitoredWork work = new MonitoredWork(
                    StackTraceLogging.LONG, 
                    "Keep jobs " + m_jobIds + " alive on targets" );
            try
            {
                runInternal();
            }
            finally
            {
                completionNotifier.completed();
                work.completed();
                LOG.info( "Finished keeping jobs " + m_jobIds + " alive on targets." );
            }
        }
        
        
        private void runInternal()
        {
            for ( final Ds3Target target : m_brm.getRetriever( Ds3Target.class ).retrieveAll().toSet() )
            {
                try
                {
                    final Ds3Connection connection = m_ds3ConnectionFactory.connect( null, target );
                    try
                    {
                        for ( final UUID jobId : m_jobIds )
                        {
                            try
                            {
                                connection.keepJobAlive( jobId );
                            }
                            catch ( final RuntimeException ex )
                            {
                                Validations.verifyNotNull( "Shut up CodePro.", ex );
                                LOG.info( "Failed to keep job " + jobId + " alive on target " 
                                          + target.getId() + " (" + target.getName() 
                                          + "), likely since it doesn't exist on said target." );
                            }
                        }
                    }
                    finally
                    {
                        connection.shutdown();
                    }
                }
                catch ( final RuntimeException ex )
                {
                    Validations.verifyNotNull( "Shut up CodePro.", ex );
                    LOG.info( "Failed to keep jobs " + m_jobIds + " alive on target " + target.getId() 
                            + " (" + target.getName() + "), since failed to connect to target." );
                }
            }
        }
        
        
        private final Set< UUID > m_jobIds = new HashSet<>();
    } // end inner class def
    

    private final static class KeepJobAliveOnTargetsAggregator
        implements ThrottledRunnable.ThrottledRunnableAggregator< KeepJobAliveOnTargetsWorker >
    {
        public void aggregate(
                final KeepJobAliveOnTargetsWorker throttledRunnableScheduledToExecute,
                final KeepJobAliveOnTargetsWorker throttledRunnableToAggregateWith )
        {
            throttledRunnableScheduledToExecute.addJobIds( throttledRunnableToAggregateWith.m_jobIds );
        }
    } // end inner class def
    
    
    private final class ActivityOccurredProcessor implements Runnable
    {
        public void run()
        {
            ActivitySpecification as = null;
            while ( !m_activitiesOccurred.isEmpty() )
            {
                try
                {
                    as = m_activitiesOccurred.take();
                    activityOccurred( as );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( "Failed to process: " + as, ex );
                }
            }
        }
    } // end inner class def
    
    
    private final static class ActivitySpecification
    {
        private ActivitySpecification( final UUID jobId, final UUID blobId )
        {
            m_jobId = jobId;
            m_blobId = blobId;
        }
        
        private final UUID m_jobId;
        private final UUID m_blobId;
    } // end inner class def
    
    
    private final int m_millisSinceLastActivityToConsiderJobDead;
    private final BeansRetrieverManager m_brm;
    private final Ds3ConnectionFactory m_ds3ConnectionFactory;
    private final Map< UUID, Duration > m_jobs = new HashMap<>();
    private final RecurringRunnableExecutor m_activityOccurredProcessorExecutor =
            new RecurringRunnableExecutor( new ActivityOccurredProcessor(), 100 );
    private final ArrayBlockingQueue< ActivitySpecification > m_activitiesOccurred;
    private final Map< UUID, Duration > m_durationSinceJobKeptAliveOnTargets = new HashMap<>();
    private final ThrottledRunnableExecutor< KeepJobAliveOnTargetsWorker > m_keepJobAliveOnTargetsExecutor;
    
    private final static Logger LOG = Logger.getLogger( DeadJobMonitorImpl.class );
}
