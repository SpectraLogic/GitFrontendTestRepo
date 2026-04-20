/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.verify;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.PublicCloudDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.target.PublicCloudBucketNameService;
import com.spectralogic.s3.common.dao.service.target.TargetFailureService;
import com.spectralogic.s3.common.dao.service.temp.BlobTargetToVerifyService;
import com.spectralogic.s3.common.platform.target.PublicCloudBucketSupport;
import com.spectralogic.s3.common.platform.target.PublicCloudBucketSupportImpl;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnectionFactory;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

public final class PublicCloudTargetVerifier
    < T extends PublicCloudReplicationTarget< T > & DatabasePersistable, 
      R extends PublicCloudDataReplicationRule< ? > & DatabasePersistable, 
      C extends PublicCloudConnection >
    implements Runnable
{
    public PublicCloudTargetVerifier(
            final Class< T > targetType,
            final Class< R > ruleType,
            final UUID targetId,
            final PublicCloudConnectionFactory< C, T > connectionFactory,
            final BeansServiceManager serviceManager,
            final Class< ? extends BlobTargetToVerifyService< ? > > blobTargetToVerifyServiceType,
            final Class< ? extends PublicCloudBucketNameService< ? > > cloudBucketNameServiceType,
            final Class< ? extends TargetFailureService< ? > > targetFailureServiceType )
    {
        Validations.verifyNotNull( "Target type", targetType );
        Validations.verifyNotNull( "Rule type", ruleType );
        Validations.verifyNotNull( "Target id", targetId );
        Validations.verifyNotNull( "Connection factory", connectionFactory );
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Blob target to verify service type", blobTargetToVerifyServiceType );
        Validations.verifyNotNull( "Cloud bucket name service type", cloudBucketNameServiceType );
        Validations.verifyNotNull( "Target failure service type", targetFailureServiceType );
        
        m_targetType = targetType;
        m_ruleType = ruleType;
        m_targetId = targetId;
        m_blobTargetToVerifyService = serviceManager.getService( blobTargetToVerifyServiceType );
        m_targetFailureService = serviceManager.getService( targetFailureServiceType );
        m_serviceManager = serviceManager;
        m_connectionFactory = connectionFactory;
        m_cloudBucketSupport =
                new PublicCloudBucketSupportImpl( cloudBucketNameServiceType, m_serviceManager );
        
        m_connectionFactory.connect( m_serviceManager.getRetriever( targetType ).attain( targetId ) )
            .shutdown();
    }
    
    
    public void run()
    {
        if ( !VERIFY_IN_PROGRESS.compareAndSet( false, true ) )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                    "Verify already in progress and only one verify can occur at a time." );
        }
        try
        {
            m_blobTargetToVerifyService.verifyBegun( m_targetId );
            runInternal();
            m_blobTargetToVerifyService.verifyCompleted( m_targetId );
            m_targetFailureService.create(m_targetId, TargetFailureType.VERIFY_COMPLETE, "Verify Complete", null);
        }
        catch ( final RuntimeException ex )
        {
            m_targetFailureService.create( m_targetId, TargetFailureType.VERIFY_FAILED, ex, null );
            throw ex;
        }
        finally
        {
            VERIFY_IN_PROGRESS.set( false );
        }
    }
    
    
    private void runInternal()
    {
        final T target = m_serviceManager.getRetriever( m_targetType ).attain( m_targetId );
        FailureTypeObservableException verificationFailure = null;
        for ( final Bucket bucket : m_serviceManager.getRetriever( Bucket.class ).retrieveAll( Require.exists(
                Bucket.DATA_POLICY_ID,
                Require.exists( 
                        m_ruleType, 
                        DataPlacement.DATA_POLICY_ID,
                        Require.beanPropertyEquals( 
                                DataReplicationRule.TARGET_ID, 
                                m_targetId ) ) ) ).toSet() )
        {
            LOG.info( "Verifying bucket " + bucket.getName() + "..." );
            try
            {
                runInternal( target, bucket );
            }
            catch ( final FailureTypeObservableException e )
            {
                LOG.warn( "Failed to verify bucket " + bucket.getName() + ".", e );
                if ( verificationFailure == null )
                {
                    verificationFailure = e;
                }
            }
            LOG.info( "Verified bucket " + bucket.getName() + "." );
        }
        if ( verificationFailure != null )
        {
            throw verificationFailure;
        }
    }
    
    
    private void runInternal( final T target, final Bucket bucket )
    {
        final C connection = m_connectionFactory.connect( target );
        try
        {
            final PublicCloudBucketInformation cloudBucket =
                    m_cloudBucketSupport.verifyBucket( connection, bucket.getId(), target.getId() );
            if ( null == cloudBucket )
            {
                LOG.warn( "Cloud bucket not found for bucket " + bucket.getName() + "." );
                return;
            }
            
            String marker = null;
            final boolean replicateDeletes =
                    isReplicateDeletes( target.getId(), bucket.getDataPolicyId() );
            do
            {
                final BucketOnPublicCloud segment = connection.discoverContents( cloudBucket, marker );
                final Set< UUID > blobIds = extractBlobs( segment );
                final Set< UUID > unknownBlobIds = m_blobTargetToVerifyService.blobsVerified( 
                        target.getId(), blobIds );
                if ( replicateDeletes && !unknownBlobIds.isEmpty() )
                {
                    m_unknownObjects.addAll( extractObjects( segment, unknownBlobIds ) );
                    if ( 10000 < m_unknownObjects.size() )
                    {
                        replicateDeletes( cloudBucket, connection );
                    }
                }
                marker = segment.getNextMarker();
            } while ( null != marker );
            
            if ( replicateDeletes )
            {
                replicateDeletes( cloudBucket, connection );
            }
            connection.syncUploads( cloudBucket.getName() );
        }
        finally
        {
            connection.shutdown();
        }
    }
    
       
    private boolean isReplicateDeletes( final UUID targetId, final UUID dataPolicyId )
    {
        return ( 0 < m_serviceManager.getRetriever( m_ruleType ).getCount(
                Require.all(
                        Require.beanPropertyEquals( DataReplicationRule.TARGET_ID, targetId ),
                        Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ),
                        Require.beanPropertyEquals( DataReplicationRule.REPLICATE_DELETES, Boolean.TRUE )
                ) ) );
    }
    
    
    private Set< UUID > extractBlobs( final BucketOnPublicCloud segment )
    {
        final Set< UUID > retval = new HashSet<>();
        for ( final S3ObjectOnMedia oom : segment.getObjects() )
        {
            for ( final BlobOnMedia bom : oom.getBlobs() )
            {
                retval.add( bom.getId() );
            }
        }
        return retval;
    }
    
    
    private Set< S3ObjectOnMedia > extractObjects( final BucketOnPublicCloud segment, final Set< UUID > unknownBlobIds )
    {
        final Set< S3ObjectOnMedia > retval = new HashSet<>();
        for ( final S3ObjectOnMedia oom : segment.getObjects() )
        {
            for ( final BlobOnMedia bom : oom.getBlobs() )
            {
                if ( unknownBlobIds.contains( bom.getId() ) )
                {
                    retval.add(oom);
                }
            }
        }
        
        return retval;
    }
    
    
    private void replicateDeletes(
            final PublicCloudBucketInformation cloudBucket,
            final PublicCloudConnection connection )
    {
        if ( m_unknownObjects.isEmpty() )
        {
            return;
        }
        
        connection.delete( cloudBucket, m_unknownObjects );
        m_unknownObjects.clear();
    }
    
    
    private final Set< S3ObjectOnMedia > m_unknownObjects = new HashSet<>();
    
    private final Class< T > m_targetType;
    private final Class< R > m_ruleType;
    private final UUID m_targetId;
    private final PublicCloudConnectionFactory< C, T > m_connectionFactory;
    private final BeansServiceManager m_serviceManager;
    private final BlobTargetToVerifyService< ? > m_blobTargetToVerifyService;
    private final TargetFailureService< ? > m_targetFailureService;
    private final PublicCloudBucketSupport m_cloudBucketSupport;
    
    private final static AtomicBoolean VERIFY_IN_PROGRESS = new AtomicBoolean( false );
    private final static Logger LOG = Logger.getLogger( PublicCloudTargetVerifier.class );
}
