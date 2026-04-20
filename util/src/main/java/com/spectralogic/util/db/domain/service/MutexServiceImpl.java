/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain.service;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.domain.Mutex;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseDatabaseBeansRetriever;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

final class MutexServiceImpl extends BaseDatabaseBeansRetriever< Mutex > implements MutexService
{
    MutexServiceImpl()
    {
        super( Mutex.class, GenericFailure.NOT_FOUND );
        m_mutexHeartbeatSenderExecutor.start();
    }

    
    public UUID acquireLock( final String lockName )
    {
        return acquireLock( lockName, Mutex.HEARTBEAT_TIMEOUT_IN_SECS * 1000 );
    }
    
    
    UUID acquireLock( final String lockName, final int lockTimeoutInMillis )
    {
        Validations.verifyNotNull( "Lock name", lockName );
        
        LOG.debug( "Acquiring global lock '" + lockName + "'..." );
        
        final Mutex mutex = BeanFactory.newBean( Mutex.class )
                .setApplicationIdentifier( m_applicationIdentifier )
                .setName( lockName );

        int sleepTimeInMillis = 5;
        Mutex existingMutex = null;
        Duration duration = new Duration();
        final MonitoredWork work = new MonitoredWork( 
                StackTraceLogging.NONE, "Acquire global lock " + lockName );
        try
        {
            while ( true )
            {
                try
                {
                    final Mutex foundMutex = retrieve( Mutex.NAME, lockName );
                    if ( null == foundMutex )
                    {
                        getDataManager().createBean( mutex );
                        return mutex.getId();
                    }
                    if ( null == existingMutex 
                            || foundMutex.getLastHeartbeat().getTime() 
                               != existingMutex.getLastHeartbeat().getTime() )
                    {
                        duration = new Duration();
                        existingMutex = foundMutex;
                    }
                    if ( lockTimeoutInMillis < duration.getElapsedMillis() )
                    {
                        LOG.warn( "Lock didn't release after " + duration + ".  Will forcibly release it." );
                        getDataManager().deleteBeans( 
                                Mutex.class, 
                                Require.beanPropertyEquals( Mutex.NAME, lockName ) );
                        try
                        {
                            Thread.sleep( 1000 );
                        }
                        catch ( final InterruptedException ex2 )
                        {
                            throw new RuntimeException( ex2 );
                        }
                    }
                }
                catch ( final DaoException ex )
                {
                    LOG.debug( "Mutex not available yet.", ex );
                }
                
                try
                {
                    Thread.sleep( sleepTimeInMillis );
                    if ( 1000 > sleepTimeInMillis )
                    {
                        sleepTimeInMillis *= 2;
                    }
                }
                catch ( InterruptedException ex1 )
                {
                    throw new RuntimeException( ex1 );
                }
            }
        }
        finally
        {
            work.completed();
        }
    }
    

    public void releaseLock( final UUID mutexId )
    {
        Validations.verifyNotNull( "Mutex id", mutexId );
        
        final Mutex mutex = attain( mutexId );
        getDataManager().deleteBean( Mutex.class, mutexId );
        final long duration = System.currentTimeMillis() - mutex.getDateCreated().getTime();
        final String message = 
                "Released global lock '" + mutex.getName() + "' (was held for " + duration + "ms).";
        if ( 100 < duration )
        {
            LOG.info( message + "  Global locks should be held for as short a duration as possible." );
        }
        else
        {
            LOG.debug( message );
        }
    }
    
    
    public void run( final String lockName, final Runnable r )
    {
        Validations.verifyNotNull( "Lock name", lockName );
        Validations.verifyNotNull( "Runnable", r );
        
        UUID mutexId = null;
        final int originalPriority = Thread.currentThread().getPriority();
        try
        {
            mutexId = acquireLock( lockName );
            Thread.currentThread().setPriority( Thread.MAX_PRIORITY );
            m_mutexHeartbeatSender.m_mutexesToHeartbeat.add( mutexId );
            r.run();
        }
        finally
        {
            m_mutexHeartbeatSender.m_mutexesToHeartbeat.remove( mutexId );
            Thread.currentThread().setPriority( originalPriority );
            try
            {
                releaseLock( mutexId );
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( "Failed to release lock.  The lock could have been deleted while being held.", ex );
            }
        }
    }
    
    
    private final class MutexHeartbeatSender implements Runnable
    {
        public void run()
        {
            final Set< UUID > ids = new HashSet<>( m_mutexesToHeartbeat );
            if ( ids.isEmpty() )
            {
                return;
            }
            
            final Set< WhereClause > whereClauses = new HashSet<>();
            for ( final UUID id : ids )
            {
                whereClauses.add( Require.beanPropertyEquals( Identifiable.ID, id ) );
            }
            
            getDataManager().updateBeans(
                    CollectionFactory.toSet( Mutex.LAST_HEARTBEAT ), 
                    BeanFactory.newBean( Mutex.class ).setLastHeartbeat( new Date() ),
                    Require.any( whereClauses ) );
        }
        
        private final Set< UUID > m_mutexesToHeartbeat = new CopyOnWriteArraySet<>();
    } // end inner class def
    
    
    private final UUID m_applicationIdentifier = UUID.randomUUID();
    private final MutexHeartbeatSender m_mutexHeartbeatSender = new MutexHeartbeatSender();
    private final RecurringRunnableExecutor m_mutexHeartbeatSenderExecutor =
            new RecurringRunnableExecutor( m_mutexHeartbeatSender, Mutex.HEARTBEAT_INTERVAL_IN_SECS * 1000 );
}
