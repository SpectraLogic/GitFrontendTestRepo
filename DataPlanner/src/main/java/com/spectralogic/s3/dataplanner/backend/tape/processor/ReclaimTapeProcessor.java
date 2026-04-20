/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.orm.StorageDomainMemberRM;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

public final class ReclaimTapeProcessor extends BaseShutdownable
{
    public ReclaimTapeProcessor( 
            final BeansServiceManager serviceManager, 
            final TapeBlobStore blobStore, 
            final Object taskStateLock,
            final TapeLockSupport< ? > tapeLockSupport,
            final int intervalInMillisForReclaim )
    {
        m_serviceManager = serviceManager;
        m_blobStore = blobStore;
        m_taskStateLock = taskStateLock;
        m_tapeLockSupport = tapeLockSupport;
        m_executor = new RecurringRunnableExecutor( m_worker, intervalInMillisForReclaim );
        
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        Validations.verifyNotNull( "Blob store", m_blobStore );
        Validations.verifyNotNull( "Task state lock", m_taskStateLock );
        Validations.verifyNotNull( "Tape lock support", m_tapeLockSupport );
        
        addShutdownListener( m_executor );
        
        m_executor.start();
    }
    
    
    /**
     *  Package private intentionally for test purposes only.
     */
    void run()
    {
        m_worker.run();
    }
    
    
    
    private final class EmptyTapeReclaimerWorker implements Runnable
    {
        @Override
        public void run()
        {
            final MonitoredWork work = 
                    new MonitoredWork( StackTraceLogging.SHORT, "Reclaiming empty tapes" );
            m_serviceManager.getService( BucketService.class ).getLock().readLock().lock();
            m_serviceManager.getService( S3ObjectService.class ).getLock().readLock().lock();
            try
            {
                synchronized ( m_taskStateLock )
                {
                    synchronized ( m_tapeLockSupport )
                    {
                        runInternal();
                    }
                }
            }
            finally
            {
                m_serviceManager.getService( S3ObjectService.class ).getLock().readLock().unlock();
                m_serviceManager.getService( BucketService.class ).getLock().readLock().unlock();
                work.completed();
            }
        }
    } // end inner class def
    
    
    private void runInternal()
    {
        final Set< Tape > emptyTapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( Require.all( 
                Require.beanPropertyEquals( PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, Boolean.TRUE ),
                Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ),
                Require.not( Require.exists( BlobTape.class, BlobTape.TAPE_ID, Require.nothing() ) ) ) )
                .toSet();
        if ( emptyTapes.isEmpty() )
        {
            return;
        }
        
        //map of storage domain member ID's to storage domains
        final Map< UUID, StorageDomain > storageDomains = new HashMap<>(); 
        for ( final Tape tape : emptyTapes )
        {
            if ( null == tape.getStorageDomainMemberId() )
            {
                LOG.warn( "Tape " + tape.getId() + " (" + tape.getBarCode() + ") was marked as assigned to a storage"
                        + " domain but is not assigned to any particular storage domain member." );    
            }
            else
            {
                final StorageDomainMemberRM sdmRM = new TapeRM( tape, m_serviceManager ).getStorageDomainMember();
                final StorageDomain sd = sdmRM.getStorageDomain().unwrap();
                storageDomains.put( sdmRM.getId(), sd );
            }
        }
        for ( final Tape tape : emptyTapes )
        {
            if ( tape.isWriteProtected() )
            {
                LOG.info( "Tape " + getTapeDescription( tape ) 
                        + " cannot be formatted since it is write protected." );
                continue;
            }
            if ( null != tape.getStorageDomainMemberId() 
                    && storageDomains.get( tape.getStorageDomainMemberId() ).isSecureMediaAllocation() )
            {
                if ( null == tape.getAvailableRawCapacity() || null == tape.getTotalRawCapacity() )
                {
                    continue;
                }
                final double availableRatio = 
                        tape.getAvailableRawCapacity().longValue() 
                        / (double)tape.getTotalRawCapacity().longValue();
                if ( TAPE_EMPTY_THRESHOLD < availableRatio )
                {
                    LOG.debug( "Will not reclaim tape " + getTapeDescription( tape ) 
                            + " since it is securely allocated." );
                    continue;
                }
                LOG.info( "Securely allocated empty tape " + getTapeDescription( tape ) 
                          + " is eligible for format since its available ratio is " + availableRatio + "." );
            }
            
            final Object lockHolder = m_tapeLockSupport.getTapeLockHolder( tape.getId() );
            if ( null != lockHolder )
            {
                LOG.info(
                        "Cannot reclaim tape " + getTapeDescription( tape ) 
                        + " at this time since it is currently locked by " + lockHolder + "." );
                continue;
            }
            
            try
            {
                m_blobStore.formatTape( BlobStoreTaskPriority.LOW, tape.getId(), true, false);
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( "Failed to reclaim tape " + tape + " at this time.  " 
                          + "Will retry later, assuming it remains eligible for reclamation.", ex );
            }
        }
    }
    
    private static String getTapeDescription( final Tape tape )
    {
        return tape.getId() + " (" + tape.getBarCode() + ")";
    }
    
    
    private final BeansServiceManager m_serviceManager;
    private final TapeBlobStore m_blobStore;
    private final EmptyTapeReclaimerWorker m_worker = new EmptyTapeReclaimerWorker();
    private final Object m_taskStateLock;
    private final TapeLockSupport< ? > m_tapeLockSupport;
    private final RecurringRunnableExecutor m_executor;
            
    private final static Logger LOG = Logger.getLogger( ReclaimTapeProcessor.class );
    private final static double TAPE_EMPTY_THRESHOLD = .97;
}
