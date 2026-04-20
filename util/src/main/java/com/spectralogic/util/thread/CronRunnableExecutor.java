/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.quartz.CronExpression;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

public final class CronRunnableExecutor
{
    private CronRunnableExecutor()
    {
        // singleton
    }
    
    
    public static void verify( final String cronExpression )
    {
        try
        {
            CronExpression.validateExpression( cronExpression );
        }
        catch ( final ParseException ex )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "Cron expression invalid or not supported: " + cronExpression, ex );
        }
    }
    
    
    public static void schedule( 
            final CronRunnableIdentifier identifier, 
            final String cronExpression,
            final Runnable ... runnables )
    {
        initScheduler();
        schedule( identifier, cronExpression, CollectionFactory.toSet( runnables ) );
    }
    
    
    public static void schedule( 
            final CronRunnableIdentifier identifier, 
            final String cronExpression,
            final Set< Runnable > runnables )
    {
        Validations.verifyNotNull( "Identifier", identifier );
        verify( cronExpression );
        unschedule( identifier );
        
        synchronized ( JOBS )
        {
            final CronJobDetail job = new CronJobDetail( identifier, cronExpression, runnables );
            try
            {
                final CronTriggerImpl cronTrigger = new CronTriggerImpl();
                cronTrigger.setName( "Trigger-" + identifier.toString() );
                cronTrigger.setCronExpression( job.m_cronExpression );
                s_scheduler.scheduleJob( job, cronTrigger );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            JOBS.put( identifier, job );
            LOG.info( "Scheduled " + identifier + " (will run for the first time at " 
                      + job.m_cronExpression.getNextValidTimeAfter( new Date() ) + ")." );
        }
    }
    
    
    /**
     * @return true if at least one {@link Runnable} was unscheduled as a result of the call
     */
    public static boolean unschedule( final CronRunnableIdentifier identifier )
    {
        synchronized ( JOBS )
        {
            final CronJobDetail job = JOBS.remove( identifier );
            if ( null != job )
            {
                try
                {
                    if ( !s_scheduler.deleteJob( job.m_key ) )
                    {
                        throw new RuntimeException(
                                "Job not found in scheduler to delete: " + job.m_identifier.toString() );
                    }
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( ex );
                }
                LOG.info( "Unscheduled " + identifier + "." );
            }
            return ( null != job );
        }
    }
    
    
    public static boolean isScheduled( final CronRunnableIdentifier identifier )
    {
        synchronized ( JOBS )
        {
            return JOBS.containsKey( identifier );
        }
    }
    
    
    @DisallowConcurrentExecution
    public final static class CronJob implements Job
    {
        @Override
        public void execute( final JobExecutionContext context ) throws JobExecutionException
        {
            ( (CronJobDetail)context.getJobDetail() ).execute( context );
        }
    } // end inner class def
    
    
    private final static class CronJobDetail extends JobDetailImpl implements Job
    {
        private CronJobDetail( 
                final CronRunnableIdentifier identifier,
                final String cronExpression,
                final Set< Runnable > runnables )
        {
            m_key = new JobKey( identifier.toString() );
            setJobClass( CronJob.class );
            setName( identifier.toString() );
            setKey( m_key );
            
            try
            {
                m_cronExpression = new CronExpression( cronExpression );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            
            m_runnables = runnables;
            m_identifier = identifier;
            Validations.verifyNotNull( "Runnables", m_runnables );
            Validations.verifyInRange( "Runnables", 1, Integer.MAX_VALUE, m_runnables.size() );
        }


        @Override
        public void execute( final JobExecutionContext context ) throws JobExecutionException
        {
            final Duration duration = new Duration();
            LOG.info( "Running " + m_identifier + "..." );
            final MonitoredWork work =
                    new MonitoredWork( StackTraceLogging.LONG, "Running " + m_identifier + "..." );
            try
            {
                for ( final Runnable r : m_runnables )
                {
                    try
                    {
                        r.run();
                    }
                    catch ( final Exception ex )
                    {
                        LOG.error( "Failed to execute " + r.getClass().getName() + ".", ex );
                    }
                }
            }
            finally
            {
                work.completed();
                LOG.info( m_identifier + " completed in " + duration + " (will run next at " 
                          + m_cronExpression.getNextValidTimeAfter( new Date() ) + ")." );
            }
        }
        
        
        private final CronRunnableIdentifier m_identifier;
        private final Set< Runnable > m_runnables;
        private final JobKey m_key;
        private final CronExpression m_cronExpression;
    } // end inner class def
    
    
     /**
      * This init approach is used to work most easily in conjunction with test
      * run thread lead detection and prevention. Please don't change it unless
      * you're explicitly working on test run thread leak detection/prevention.
      */
    private static void initScheduler()
    {
        synchronized( FACTORY )
        {
            try
            {
                if ( null == s_scheduler || s_scheduler.isShutdown() )
                {
                    s_scheduler = FACTORY.getScheduler();
                    s_scheduler.start();
                }
            }
            catch ( SchedulerException ex )
            {
                throw new RuntimeException( "Failed to init Scheduler", ex );
            }
        }
    }
    
    private final static SchedulerFactory FACTORY;
    private static Scheduler s_scheduler;
    static
    {
        try
        {
            FACTORY = new StdSchedulerFactory();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to initialize.", ex );
        }
    }
    
    
    private static final Map< CronRunnableIdentifier, CronJobDetail > JOBS = new HashMap<>();
    private final static Logger LOG = Logger.getLogger( CronRunnableExecutor.class );
}
