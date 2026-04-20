/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import java.util.Date;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

public final class PeriodicTargetVerifier extends BaseShutdownable
{
    public PeriodicTargetVerifier( 
            final BeansServiceManager serviceManager,
            final TargetManagementResource resource,
            final int runFrequencyInMillis )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Resource", resource );
        Validations.verifyInRange( "Run frequency in millis", 10, 1000 * 3600 * 24, runFrequencyInMillis );
        
        m_serviceManager = serviceManager;
        m_resource = resource;
        m_executor = new RecurringRunnableExecutor( m_periodicTargetVerifierWorker, runFrequencyInMillis );
        m_executor.start();
        addShutdownListener( m_executor );
    }
    
    
    private final class PeriodicTargetVerifierWorker implements Runnable
    {
        public void run()
        {
            verifyTargetsAsNecessary( AzureTarget.class );
            verifyTargetsAsNecessary( S3Target.class );
        }
    } // end inner class def
    
    
    private < T extends PublicCloudReplicationTarget< ? > & DatabasePersistable > 
    void verifyTargetsAsNecessary( final Class< T > targetType )
    {
        for ( final T target : m_serviceManager.getRetriever( targetType ).retrieveAll().toSet() )
        {
            if ( null == target.getAutoVerifyFrequencyInDays() )
            {
                continue;
            }
            if ( null != target.getLastFullyVerified() 
                    && target.getLastFullyVerified().getTime() 
                       + target.getAutoVerifyFrequencyInDays().intValue() * 1000L * 3600 * 24 
                       > System.currentTimeMillis() )
            {
                continue;
            }
            
            try
            {
                LOG.info( "Auto-verification required for " + getTargetDescription( target ) + "." );
                m_resource.verifyPublicCloudTarget( targetType, target.getId(), true );
                target.setLastFullyVerified( new Date() );
                m_serviceManager.getUpdater( targetType ).update(
                        target,
                        PublicCloudReplicationTarget.LAST_FULLY_VERIFIED );
                LOG.info( "Auto-verification completed for " + getTargetDescription( target ) + "." );
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( "Failed to verify " + getTargetDescription( target ) + " at this time.", ex );
            }
        }
    }
    
    
    private < T extends PublicCloudReplicationTarget< ? > & DatabasePersistable > 
    String getTargetDescription( final T target )
    {
        return InterfaceProxyFactory.getType( target.getClass() ).getSimpleName() + " " + target.getName()
               + " (" + target.getId() + ")";
    }
    
    
    private final BeansServiceManager m_serviceManager;
    private final TargetManagementResource m_resource;
    private final RecurringRunnableExecutor m_executor;
    private final Runnable m_periodicTargetVerifierWorker = new PeriodicTargetVerifierWorker();
    
    private final static Logger LOG = Logger.getLogger( PeriodicTargetVerifier.class );
}
