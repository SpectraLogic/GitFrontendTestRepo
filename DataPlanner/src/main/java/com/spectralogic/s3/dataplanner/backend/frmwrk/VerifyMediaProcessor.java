/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

public final class VerifyMediaProcessor< T extends DatabasePersistable & PersistenceTarget< T >,
                                         B extends DatabasePersistable,
                                         S extends BlobStoreTask > extends BaseShutdownable
{
    public VerifyMediaProcessor( 
            final Class< T > mediaType,
            final WhereClause mediaTypeFilterForEligibleMediaToVerify,
            final Class< B > blobPersistenceType,
            final Class< S > blobStoreTaskType,
            final BeansServiceManager serviceManager, 
            final BlobStore blobStore, 
            final int intervalBetweenVerifySchedulingInMillis,
            final int maxNumberOfVerifyTasksAtOnce )
    {
        m_mediaType = mediaType;
        m_mediaTypeFilterForEligibleMediaToVerify = mediaTypeFilterForEligibleMediaToVerify;
        m_blobPersistenceType = blobPersistenceType;
        m_blobStoreTaskType = blobStoreTaskType;
        m_maxNumberOfVerifyTasksAtOnce = maxNumberOfVerifyTasksAtOnce;
        m_serviceManager = serviceManager;
        m_blobStore = blobStore;
        m_executor = new RecurringRunnableExecutor(
                m_scheduler, 
                intervalBetweenVerifySchedulingInMillis );
        addShutdownListener( m_executor );
        m_executor.start();

        Validations.verifyNotNull( "Media type", m_mediaType );
        Validations.verifyNotNull( "Media filter", m_mediaTypeFilterForEligibleMediaToVerify );
        Validations.verifyNotNull( "Blob persistence type", m_blobPersistenceType );
        Validations.verifyNotNull( "Task type", m_blobStoreTaskType );
        Validations.verifyInRange(
                "Max number of verify tasks at once", 1, 1024, m_maxNumberOfVerifyTasksAtOnce );
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        Validations.verifyNotNull( "Blob store", m_blobStore );
    }
    
    
    private final class VerifyMediaScheduler implements Runnable
    {
        @Override
        public void run()
        {
            final Set< T > mediasRequiringVerification = new HashSet<>();
            for ( final StorageDomain storageDomain
                    : m_serviceManager.getRetriever( StorageDomain.class ).retrieveAll().toSet() )
            {
                if ( null == storageDomain.getMaximumAutoVerificationFrequencyInDays() )
                {
                    continue;
                }
                
                final Date maxLastModified = new Date( System.currentTimeMillis() 
                        - storageDomain.getMaximumAutoVerificationFrequencyInDays()
                        .intValue() * 1000L * 3600 * 24 );
                final Set< T > medias = m_serviceManager.getRetriever( m_mediaType ).retrieveAll( Require.all(
                        m_mediaTypeFilterForEligibleMediaToVerify,
                        Require.beanPropertyLessThan(
                                PersistenceTarget.LAST_MODIFIED, 
                                maxLastModified ),
                        Require.exists(
                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                                Require.beanPropertyEquals(
                                        StorageDomainMember.STORAGE_DOMAIN_ID,
                                        storageDomain.getId() ) ),
                        Require.beanPropertyEquals( PersistenceTarget.LAST_VERIFIED, null ),
                        Require.exists(
                                m_blobPersistenceType, 
                                m_mediaType.getSimpleName().toLowerCase() + "Id",
                                Require.nothing() ) ) ).toSet();
                if ( !medias.isEmpty() )
                {
                    LOG.info( "Will auto-verify " + medias.size() + " " 
                              + m_mediaType.getSimpleName() + "s in storage domain "
                              + storageDomain.getId() + " (" + storageDomain.getName()
                              + ") that were last written prior to "
                              + maxLastModified + "." );
                }
                mediasRequiringVerification.addAll( medias );
            }
            
            scheduleVerification( mediasRequiringVerification );
        }
        
        
        private void scheduleVerification( final Set< T > medias )
        {
            LOG.info( medias.size() + " " + m_mediaType.getSimpleName().toLowerCase()
                      + "s require verification." );
            if ( medias.isEmpty() )
            {
                return;
            }
            
            final int numVerifiesAlreadyScheduled = getNumberOfVerifyTasks();
            int numVerifiesScheduled = 0;
            int numFailures = 0;
            for ( final T media : medias )
            {
                if ( numVerifiesAlreadyScheduled + numVerifiesScheduled >= m_maxNumberOfVerifyTasksAtOnce )
                {
                    continue;
                }
                
                try
                {
                    m_blobStore.verify( BlobStoreTaskPriority.BACKGROUND, media.getId() );
                    ++numVerifiesScheduled;
                }
                catch ( final RuntimeException ex )
                {
                    ++numFailures;
                    LOG.debug( "Failed to schedule a verification for " 
                              + m_mediaType.getSimpleName().toLowerCase() 
                              + media.getId() + " at this time.", ex );
                }
            }
            LOG.info( "Scheduled " + numVerifiesScheduled + " verifies (out of " 
                      + ( numFailures + numVerifiesScheduled ) + " eligible " 
                      + NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert(
                              m_mediaType.getSimpleName() )
                      + "s)." );
        }
    } // end inner class def
    
    
    private int getNumberOfVerifyTasks()
    {
        int retval = 0;
        for ( final BlobStoreTask t : m_blobStore.getTasks() )
        {
            if ( m_blobStoreTaskType.isAssignableFrom( t.getClass() ) )
            {
                ++retval;
            }
        }
        
        return retval;
    }
    

    private final Class< T > m_mediaType;
    private final WhereClause m_mediaTypeFilterForEligibleMediaToVerify;
    private final Class< B > m_blobPersistenceType;
    private final Class< S > m_blobStoreTaskType;
    private final int m_maxNumberOfVerifyTasksAtOnce;
    private final BeansServiceManager m_serviceManager;
    private final BlobStore m_blobStore;
    private final RecurringRunnableExecutor m_executor;
    private final VerifyMediaScheduler m_scheduler = new VerifyMediaScheduler();
    
    private final static Logger LOG = Logger.getLogger( VerifyMediaProcessor.class );
}
