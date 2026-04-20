/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.shared.ImportPublicCloudTargetDirective;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudTargetBucketName;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.shared.ImportDirectiveService;
import com.spectralogic.s3.common.dao.service.target.PublicCloudBucketNameService;
import com.spectralogic.s3.common.dao.service.target.TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.importer.BaseImportHandler;
import com.spectralogic.s3.dataplanner.backend.importer.PublicCloudTargetImporter;
import com.spectralogic.s3.dataplanner.backend.importer.ReplicationTargetImportHandler;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

public abstract class BaseImportTargetTask
    < T extends DatabasePersistable & PublicCloudReplicationTarget< T >,
      BT extends DatabasePersistable & BlobTarget< BT >,
      ID extends ImportPublicCloudTargetDirective< ID > & DatabasePersistable,
      CF extends PublicCloudConnectionFactory< ?, T >,
      BN extends PublicCloudTargetBucketName< BN > & DatabasePersistable >
    extends BasePublicCloudTask< T, CF >
{
    protected BaseImportTargetTask(
            final Class< T > targetType,
            final Class< ? extends PublicCloudBucketNameService< ? > > cloudBucketNameServiceType,
            final Class< ? extends TargetFailureService< ? > > failureServiceType,
            final Class< BT > blobTargetType,
            final Class< ? extends ImportDirectiveService< ID > > importDirectiveServiceType,
            final Class< BN > targetBucketNameType,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager,
            final CF connectionFactory,
            final BlobStoreTaskPriority priority,
            final UUID targetId )
    {
        super( cloudBucketNameServiceType,
                targetType,
                targetId,
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory,
                priority );

        m_failureServiceType = failureServiceType;
        m_blobTargetType = blobTargetType;
        m_targetId = targetId;
        m_importDirectiveServiceType = importDirectiveServiceType;
        m_targetBucketNameType = targetBucketNameType;
        Validations.verifyNotNull( "Blob target type", m_blobTargetType );
        Validations.verifyNotNull( "Target", m_targetId );
        Validations.verifyNotNull( "Import directive service type", m_importDirectiveServiceType );
        Validations.verifyNotNull( "Target bucket name type", m_targetBucketNameType );
    }


    @Override
    protected boolean prepareForExecution()
    {
        return true;
    }


    @Override
    protected BlobStoreTaskState runInternal()
    {
        final ImportTargetHandler importHandler = new ImportTargetHandler();
        final PublicCloudTargetImporter< BT, T, ID > importer =
                new PublicCloudTargetImporter<>(
                        m_blobTargetType,
                        m_targetType,
                        getTargetId(),  
                        m_importDirectiveServiceType,
                        importHandler, 
                        getServiceManager() );
        try
        {
            return importer.run();
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to import target " + getTargetId() + ".", ex );
            return importHandler.failed( TargetFailureType.IMPORT_FAILED, ex );
        }
    }
    
    
    private final class ImportTargetHandler 
        extends BaseImportHandler< TargetFailureType >
        implements ReplicationTargetImportHandler
    {
        public void openForRead()
        {
            final PublicCloudConnection connection = getConnectionFactory().connect( getTarget() );
            try
            {
                final ID directive = 
                        m_serviceManager.getService( m_importDirectiveServiceType ).attainByEntityToImport(
                                m_targetId );
                m_cloudBucket = getCloudBucketSupport().verifyBucketToImport( 
                        connection, directive.getCloudBucketName() );
            }
            finally
            {
                connection.shutdown();
            }
        }
        
        
        public S3ObjectsOnMedia read()
        {
            if ( m_done )
            {
                return null;
            }
            if ( null == m_cloudBucket )
            {
                throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "Given cloud bucket does not exist." );
            }
            
            final PublicCloudConnection connection = getConnectionFactory().connect( getTarget() );
            try
            {
                final BucketOnPublicCloud boc = connection.discoverContents( m_cloudBucket, m_nextMarker );
                m_nextMarker = boc.getNextMarker();
                m_done = ( null == m_nextMarker );
                
                final S3ObjectsOnMedia retval = BeanFactory.newBean( S3ObjectsOnMedia.class );
                retval.setBuckets( new BucketOnMedia []
                        { 
                            BeanFactory.newBean( BucketOnMedia.class )
                            .setBucketName( boc.getBucketName() )
                            .setObjects( boc.getObjects() )
                        } );
                return retval;
            }
            finally
            {
                connection.shutdown();
            }
        }

        
        public void closeRead()
        {
            // empty
        }
        
        
        public BlobStoreTaskState finalizeImport()
        {
            final PublicCloudConnection connection = getConnectionFactory().connect( getTarget() );
            try
            {
                connection.takeOwnership(
                        m_cloudBucket, 
                        m_serviceManager.getRetriever( DataPathBackend.class ).attain( Require.nothing() )
                            .getInstanceId() );
            }
            finally
            {
                connection.shutdown();
            }
            
            final BeansServiceManager transaction = getServiceManager().startTransaction();
            try
            {
                final BN customBucketNameMapping = BeanFactory.newBean( m_targetBucketNameType );
                customBucketNameMapping.setTargetId( m_targetId );
                customBucketNameMapping.setName( m_cloudBucket.getName() );
                customBucketNameMapping.setBucketId(
                        m_serviceManager.getRetriever( Bucket.class ).attain( 
                                Bucket.NAME, m_cloudBucket.getLocalBucketName() ).getId() );
                final BN existingBn =
                        transaction.getRetriever( m_targetBucketNameType ).retrieve( Require.all(
                                Require.beanPropertyEquals( 
                                        PublicCloudTargetBucketName.TARGET_ID, 
                                        m_targetId ),
                                Require.beanPropertyEquals(
                                        PublicCloudTargetBucketName.BUCKET_ID, 
                                        customBucketNameMapping.getBucketId() ) ) );
                if ( null != existingBn )
                {
                    transaction.getDeleter( m_targetBucketNameType ).delete( existingBn.getId() );
                }
                transaction.getCreator( m_targetBucketNameType ).create( customBucketNameMapping );
                transaction.getService( m_failureServiceType ).deleteAll( 
                        getTargetId(), TargetFailureType.IMPORT_FAILED );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
            return BlobStoreTaskState.COMPLETED;
        }
    
        
        @Override
        public BlobStoreTaskState failedInternal(
                final TargetFailureType failureType,
                final RuntimeException ex )
        {
            getServiceManager().getService( m_failureServiceType ).create(
                    getTargetId(), failureType, ex, null );
            return BlobStoreTaskState.COMPLETED;
        }
        
        
        @Override
        public void warnInternal(
                final TargetFailureType failureType,
                final RuntimeException ex )
        {
            getServiceManager().getService( m_failureServiceType ).create(
                    getTargetId(), failureType, ex, null );
        }
        
        
        private volatile boolean m_done;
        private volatile String m_nextMarker;
        private volatile PublicCloudBucketInformation m_cloudBucket;
    } // end inner class def


    protected void handleFailure( final Exception ex )
    {
        getServiceManager().getService( m_failureServiceType ).create(
                getTargetId(), TargetFailureType.IMPORT_FAILED, ex, null );
    }


    public String getDescription()
    {
        return "Import " + getTargetType() + " " + m_targetId;
    }


    private final UUID m_targetId;
    private final Class< BT > m_blobTargetType;
    private final Class< ? extends TargetFailureService< ? > > m_failureServiceType;
    private final Class< ? extends ImportDirectiveService< ID > > m_importDirectiveServiceType;
    private final Class< BN > m_targetBucketNameType;
}
