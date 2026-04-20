/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKey;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKeyType;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.service.ds3.FeatureKeyService;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

public final class FeatureKeyValidator extends BaseShutdownable implements Runnable
{
    public FeatureKeyValidator( final BeansServiceManager serviceManager )
    {
        this( serviceManager, Long.valueOf( 24 * 3600L * 1000 ) ); // by default, run every 24 hours
    }
    
    
    FeatureKeyValidator( final BeansServiceManager serviceManager, final Long validationFrequencyInMillis )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        m_serviceManager = serviceManager;
        m_executor = ( null == validationFrequencyInMillis ) ? 
                null 
                : new RecurringRunnableExecutor( this, validationFrequencyInMillis.longValue() );
        if ( null != m_executor )
        {
            addShutdownListener( m_executor );
            m_executor.start();
        }
    }
    
    
    public void run()
    {
        final CountDownLatch latch;
        synchronized ( this )
        {
            if ( null == m_latch )
            {
                m_latch = new CountDownLatch( 1 );
                latch = null;
            }
            else
            {
                latch = m_latch;
            }
        }
        
        if ( null != latch )
        {
            LOG.info( "Waiting on active validation invocation to complete..." );
            try
            {
                latch.await();
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
            LOG.info( "Active invocation completed." );
            return;
        }
        
        LOG.info( "Validating feature keys..." );
        try
        {
            invalidateExpiredKeys();
            validateCloudOutKeys();
            LOG.info( "Finished validating feature keys." );
        }
        finally
        {
            synchronized ( this )
            {
                m_latch.countDown();
                m_latch = null;
            }
        }
    }
    
    
    private void invalidateExpiredKeys()
    {
        LOG.info( "Invalidating expired keys..." );
        final FeatureKeyService service = m_serviceManager.getService( FeatureKeyService.class );
        for ( final FeatureKey key : service.retrieveAll( Require.all( 
                Require.beanPropertyEquals( ErrorMessageObservable.ERROR_MESSAGE, null ),
                Require.beanPropertyLessThan( FeatureKey.EXPIRATION_DATE, new Date() ) ) ).toSet() )
        {
            service.update(
                    key.setErrorMessage( "Key expired " + key.getExpirationDate() ),
                    ErrorMessageObservable.ERROR_MESSAGE );
        }
    }
    
    
    private void validateCloudOutKeys()
    {
        validateCloudOutKey(
                FeatureKeyType.AWS_S3_CLOUD_OUT,
                BlobS3Target.class );
        validateCloudOutKey(
                FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT, 
                BlobAzureTarget.class );
    }
    
    
    private < B extends DatabasePersistable & BlobTarget< B > > void validateCloudOutKey( 
            final FeatureKeyType keyType,
            final Class< B > blobTargetType )
    {
        final FeatureKey key =
                m_serviceManager.getRetriever( FeatureKey.class ).retrieve( FeatureKey.KEY, keyType );
        if ( null == key )
        {
            LOG.info( "Key not installed for " + keyType + ", so no key to validate." );
            return;
        }
        if ( null != key.getErrorMessage() )
        {
            LOG.info( "Key already invalidated for " + keyType + ", so will skip its validation." );
            return;
        }
        if ( null == key.getLimitValue() )
        {
            LOG.info( "Key installed for " + keyType + " has no limit, so will skip its validation." );
            return;
        }
        
        final BytesRenderer bytesRenderer = new BytesRenderer();
        LOG.info( "Validating key " + keyType + " (limit is " + bytesRenderer.render( key.getLimitValue() )
                  + ")..." );
        final long currentValue = m_serviceManager.getRetriever( Blob.class ).getSum(
                Blob.LENGTH, 
                Require.exists( blobTargetType, BlobObservable.BLOB_ID, Require.nothing() ) );
        m_serviceManager.getService( FeatureKeyService.class ).update(
                key.setCurrentValue( Long.valueOf( currentValue ) ), 
                FeatureKey.CURRENT_VALUE );
        final String usage = " (using " + bytesRenderer.render( key.getCurrentValue() ) 
                + " of " + bytesRenderer.render( key.getLimitValue() ) + ").";
        if ( currentValue > key.getLimitValue().longValue() )
        {
            LOG.warn( "Key " + keyType + " must be invalidated since its value exceeds its limit" + usage );
            m_serviceManager.getService( FeatureKeyService.class ).update( 
                    key.setErrorMessage(
                            "Limit (" + bytesRenderer.render( key.getLimitValue() ) + ") was exceeded." ),
                    ErrorMessageObservable.ERROR_MESSAGE );
        }
        LOG.info( "Finished validating key " + keyType + usage );
    }
    
    
    private CountDownLatch m_latch;
    
    private final RecurringRunnableExecutor m_executor;
    private final BeansServiceManager m_serviceManager;
    
    private final static Logger LOG = Logger.getLogger( FeatureKeyValidator.class );
}
