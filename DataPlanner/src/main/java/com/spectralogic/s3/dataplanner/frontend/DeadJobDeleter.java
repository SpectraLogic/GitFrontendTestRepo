/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.service.ds3.CanceledJobService;
import com.spectralogic.s3.common.rpc.dataplanner.JobResource;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

public final class DeadJobDeleter extends BaseShutdownable
{
    public DeadJobDeleter(
            final int intervalBetweenCleanupsInMillis, 
            final BeansServiceManager serviceManager,
            final DeadJobMonitor monitor, 
            final JobResource resource )
    {
        m_executor = new RecurringRunnableExecutor(
                new DeadJobCleanUpWorker( serviceManager, monitor, resource ),
                intervalBetweenCleanupsInMillis );
        
        m_executor.start();
        addShutdownListener( m_executor );
    }
    
    
    private final static class DeadJobCleanUpWorker implements Runnable
    {
        private DeadJobCleanUpWorker( 
                final BeansServiceManager serviceManager,
                final DeadJobMonitor monitor, 
                final JobResource resource )
        {
            m_canceledJobService = serviceManager.getService( CanceledJobService.class );
            m_jobRetriever = serviceManager.getRetriever( Job.class );
            m_monitor = monitor;
            m_resource = resource;
        }
        
        public void run()
        {
            m_resource.cleanUpCompletedJobsAndJobChunks();
            for ( final Job job : m_jobRetriever.retrieveAll().toSet() )
            {
                if ( m_monitor.isDead( job.getId() ) && !job.isTruncated() && job.isDeadJobCleanupAllowed() )
                {
                    LOG.warn( job + " has been inactive too long and is considered dead.  Will cancel it." );
                    try {
                        m_resource.cancelJob( null, job.getId(), false );
                        m_canceledJobService.markAsTimedOut( job.getId() );
                    } catch ( final Exception e ) {
                        LOG.error( "Failed to cancel dead job " + job.getId(), e );
                    }
                }
            }
        }

        private final CanceledJobService m_canceledJobService;
        private final BeansRetriever< Job > m_jobRetriever;
        private final DeadJobMonitor m_monitor;
        private final JobResource m_resource;
    } // end inner class def
    
    
    private final RecurringRunnableExecutor m_executor;
    private final static Logger LOG = Logger.getLogger( DeadJobDeleter.class );
}
