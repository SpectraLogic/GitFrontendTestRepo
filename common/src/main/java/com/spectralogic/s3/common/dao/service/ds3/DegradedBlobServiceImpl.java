/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectLostNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.service.suspectblob.BaseSuspectBlobTargetService;
import com.spectralogic.s3.common.platform.notification.generator.S3ObjectsLostNotificationPayloadGenerator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.notification.domain.bean.HttpNotificationEvent;

final class DegradedBlobServiceImpl extends BaseService< DegradedBlob > implements DegradedBlobService
{
    DegradedBlobServiceImpl()
    {
        super( DegradedBlob.class );
    }
    
    
    @Override
    public void create( final Set< DegradedBlob > blobs )
    {
        final int existingCount = removeExistentPersistedBeansFromSet( blobs );
        if ( 0 < existingCount )
        {
            LOG.info( existingCount + " degraded blobs were already recorded and will not be created." );
        }
        
        super.create( blobs );
    }
    
    
    @Override
    public void migrate( 
            final String degradedBlobRuleProperty,
            final UUID bucketId,
            final UUID destRuleId,
            final UUID srcRuleId )
    {
        Validations.verifyNotNull( "Bucket", bucketId );
        Validations.verifyNotNull( "Destination", destRuleId );
        Validations.verifyNotNull( "Source", srcRuleId );
        
        final DegradedBlob bean = BeanFactory.newBean( DegradedBlob.class );
        final Method writer = BeanUtils.getWriter( DegradedBlob.class, degradedBlobRuleProperty );
        try
        {
            writer.invoke( bean, destRuleId );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        
        getDataManager().updateBeans(
                CollectionFactory.toSet( degradedBlobRuleProperty ),
                bean,
                Require.all( 
                        Require.beanPropertyEquals( degradedBlobRuleProperty, srcRuleId ),
                        Require.beanPropertyEquals( DegradedBlob.BUCKET_ID, bucketId ) ) );
    }
    
    
    @Override
    public void deleteAllForPersistenceRule( final UUID dataPersistenceRuleId )
    {
        getDataManager().deleteBeans( 
                DegradedBlob.class, 
                Require.beanPropertyEquals( DegradedBlob.PERSISTENCE_RULE_ID, dataPersistenceRuleId ) );
    }
    
    
    @Override
    public void deleteForPersistenceRule( 
            final UUID dataPersistenceRuleId, 
            final Class< ? extends DatabasePersistable > persistenceTargetType,
            final String persistenceTargetPropertyName,
            final UUID persistenceTargetId,
            final Set< UUID > blobIds )
    {
        deleteForRule(
                DegradedBlob.PERSISTENCE_RULE_ID,
                dataPersistenceRuleId, 
                persistenceTargetType,
                persistenceTargetPropertyName, 
                persistenceTargetId,
                blobIds );
    }
    
    
    @Override
    public void deleteForReplicationRule( 
            final String degradedBlobPropertyName,
            final UUID ruleId, 
            final Class< ? extends DatabasePersistable > blobTargetType,
            final UUID targetId,
            final Set< UUID > blobIds )
    {
        deleteForRule(
                degradedBlobPropertyName,
                ruleId, 
                blobTargetType,
                BlobTarget.TARGET_ID, 
                targetId,
                blobIds );
    }
    
    
    public void deleteForRule( 
            final String degradedBlobPropertyName,
            final UUID ruleId, 
            final Class< ? extends DatabasePersistable > persistenceTargetType,
            final String persistenceTargetPropertyName,
            final UUID persistenceTargetId,
            final Set< UUID > blobIds )
    {
        getDataManager().deleteBeans( 
                DegradedBlob.class, 
                Require.all( 
                        Require.beanPropertyEquals( degradedBlobPropertyName, ruleId ),
                        Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ) );
        
        new SuspectBlobsDeleter( 
                persistenceTargetType,
                persistenceTargetPropertyName, 
                persistenceTargetId, 
                blobIds ).run();
    }
    
    
    private final class SuspectBlobsDeleter implements Runnable
    {
        @SuppressWarnings( "unchecked" )
        private SuspectBlobsDeleter(
                final Class< ? extends DatabasePersistable > persistenceTargetType,
                final String persistenceTargetPropertyName,
                final UUID persistenceTargetId,
                final Set< UUID > blobIds )
        {
            final String suspectPersistenceTargetTypeName =
                    persistenceTargetType.getPackage().getName() 
                    + ".Suspect" + persistenceTargetType.getSimpleName();
            final Class< ? extends DatabasePersistable > suspectPersistenceTargetType;
            try
            {
                suspectPersistenceTargetType = (Class< ? extends DatabasePersistable >)
                        Class.forName( suspectPersistenceTargetTypeName );
            }
            catch ( final ClassNotFoundException ex )
            {
                throw new RuntimeException( ex );
            }
            m_service = (BaseSuspectBlobTargetService< ? >)getServiceManager().getRetriever(
                    suspectPersistenceTargetType );
            m_persistenceTargets = 
                    getServiceManager().getRetriever( persistenceTargetType ).retrieveAll( Require.all( 
                            Require.beanPropertyEquals( persistenceTargetPropertyName, persistenceTargetId ),
                            Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ) ).toSet();
        }
        
        
        @Override
        public void run()
        {
            final Set< DatabasePersistable > segment = new HashSet<>();
            for ( final DatabasePersistable persistenceTarget : m_persistenceTargets )
            {
                segment.add( persistenceTarget );
                if ( 1000 == segment.size() )
                {
                    delete( segment );
                    segment.clear();
                }
            }
            delete( segment );
        }
        
        
        private void delete( final Set< DatabasePersistable > segment )
        {
            final Set< ? > existingBeans = m_service.retrieveAll( 
                    BeanUtils.< UUID >extractPropertyValues( segment, Identifiable.ID ) ).toSet();
            getDataManager().deleteBeans(
                    m_service.getServicedType(), 
                    Require.beanPropertyEqualsOneOf( 
                            Identifiable.ID, 
                            BeanUtils.< UUID >extractPropertyValues( existingBeans, Identifiable.ID ) ) );
            m_service.updateSuspectFailureExistence();
        }
        
        
        private final BaseSuspectBlobTargetService< ? > m_service;
        private final Set< ? extends DatabasePersistable > m_persistenceTargets;
    } // end inner class def
    
    
    @Override
    public void deleteAllForReplicationRule(
            final String replicationRulePropertyName,
            final UUID dataReplicationRuleId )
    {
        getDataManager().deleteBeans( 
                DegradedBlob.class, 
                Require.beanPropertyEquals( replicationRulePropertyName, dataReplicationRuleId ) );
    }
    
    
    @Override
    public < T extends PersistenceTarget< T >, BT extends DatabasePersistable > void blobsLostLocally( 
            final Class< T > persistenceTargetClass,
            final Class< BT > blobPersistenceTargetClass,
            final UUID persistenceTargetId,
            String error,
            final Set< UUID > blobIds )
    {
        new PersistenceTargetBlobLossRecorder<>(
                persistenceTargetClass, 
                blobPersistenceTargetClass,
                persistenceTargetId, 
                error, 
                blobIds ).run();
    }
    
    
    @Override
    public 
    < T extends DatabasePersistable & BlobTarget< T >, R extends Identifiable & DataReplicationRule< R > > 
    void blobsLostOnTarget(
            final Class< T > blobTargetType,
            final Class< R > replicationRuleType,
            final String degradedBlobReplicationRulePropertyName,
            final UUID targetId,
            String error,
            final Set< UUID > blobIds )
    {
        new TargetBlobLossRecorder<>(
                blobTargetType,
                replicationRuleType,
                degradedBlobReplicationRulePropertyName,
                targetId,
                error, 
                blobIds ).run();
    }
    
    
    private abstract class BlobLossRecorder implements Runnable
    {
        protected BlobLossRecorder(
                final String error,
                final Set< UUID > blobIds )
        {
            m_normal = ( null == error );
            m_error = ( null == error ) ? "normal operation" : error;
            m_serviceManager = getServiceManager();
            m_dataManager = getDataManager();
            m_blobIds = bucketize( blobIds );
        }
        

        private Map< UUID, Set< UUID > > bucketize( final Set< UUID > blobIds )
        {
            final Set< Blob > blobs = 
                    m_serviceManager.getRetriever( Blob.class ).retrieveAll( blobIds ).toSet();
            final Map< UUID, S3Object > objects = 
                    BeanUtils.toMap( m_serviceManager.getRetriever( S3Object.class ).retrieveAll( 
                            BeanUtils.< UUID >extractPropertyValues( blobs, Blob.OBJECT_ID ) ).toSet() );
            final Map< UUID, Set< UUID > > bucketizedBlobIds = new HashMap<>();
            for ( final Blob blob : blobs )
            {
                final S3Object o = objects.get( blob.getObjectId() );
                if ( !bucketizedBlobIds.containsKey( o.getBucketId() ) )
                {
                    bucketizedBlobIds.put( o.getBucketId(), new HashSet< UUID >() );
                }
                bucketizedBlobIds.get( o.getBucketId() ).add( blob.getId() );
            }
            
            return bucketizedBlobIds;
        }
        
        
        @Override
        final public void run()
        {
            for ( final Set< UUID > blobIds : m_blobIds.values() )
            {
                if ( !m_normal )
                {
                    if ( !m_serviceManager.isTransaction() )
                    {
                        throw new RuntimeException( 
                                "Blob loss due to error must always be done inside a transaction." );
                    }
                    
                    final DataPlacement< ? > ruleToRebuild = getRuleToRebuild( blobIds );
                    if ( null != getRuleToRebuild( blobIds ) )
                    {
                        LOG.warn( "Rebuild required for " + blobIds.size() + " lost blobs." );
                        final Map< UUID, Blob > blobs = BeanUtils.toMap( 
                                m_serviceManager.getRetriever( Blob.class ).retrieveAll( blobIds ).toSet() );
                        final Map< UUID, S3Object > objects = BeanUtils.toMap( 
                                m_serviceManager.getRetriever( S3Object.class ).retrieveAll(
                                        BeanUtils.< UUID >extractPropertyValues( 
                                                blobs.values(), Blob.OBJECT_ID ) ).toSet() );
                        final Set< DegradedBlob > degradedBlobs = new HashSet<>();
                        for ( final UUID blobId : blobIds )
                        {
                            final DegradedBlob degradedBlob = BeanFactory.newBean( DegradedBlob.class )
                                .setBucketId( objects.get( blobs.get( blobId ).getObjectId() ).getBucketId() )
                                .setBlobId( blobId );
                            populateDegradedBlob( degradedBlob, ruleToRebuild );
                            degradedBlobs.add( degradedBlob );
                        }
                        
                        m_serviceManager.getService( DegradedBlobService.class ).create( degradedBlobs );
                    }
                }
                
                commit( blobIds );
                
                if ( !m_normal )
                {
                    m_serviceManager.getNotificationEventDispatcher().fire( new HttpNotificationEvent(
                            m_serviceManager.getRetriever( S3ObjectLostNotificationRegistration.class ),
                            new S3ObjectsLostNotificationPayloadGenerator( 
                                    blobIds, 
                                    m_serviceManager.getRetriever( Bucket.class ),
                                    m_serviceManager.getRetriever( S3Object.class ),
                                    m_serviceManager.getRetriever( Blob.class ) ) ) );
                }
            }
        }
        
        
        protected abstract DataPlacement< ? > getRuleToRebuild( final Set< UUID > blobIds );
        
        
        protected abstract void populateDegradedBlob( 
                final DegradedBlob degradedBlob,
                final DataPlacement< ? > dataPlacement );
        
        
        protected abstract void commit( final Set< UUID > blobIds );
        
        
        protected final String m_error;
        private final boolean m_normal;
        private final Map< UUID, Set< UUID > > m_blobIds;
        protected final BeansServiceManager m_serviceManager;
        protected final DataManager m_dataManager;
    } // end inner class def
    
    
    private final class PersistenceTargetBlobLossRecorder
    < T extends PersistenceTarget< T >, BT extends DatabasePersistable > extends BlobLossRecorder
    {
        private PersistenceTargetBlobLossRecorder(
                final Class< T > persistenceTargetClass,
                final Class< BT > blobPersistenceTargetClass,
                final UUID persistenceTargetId,
                String error,
                final Set< UUID > blobIds )
        {
            super( error, blobIds );
            m_persistenceTargetClass = persistenceTargetClass;
            m_blobPersistenceTargetClass = blobPersistenceTargetClass;
            m_persistenceTargetId = persistenceTargetId;
        }
        
        
        @Override
        protected DataPlacement< ? > getRuleToRebuild( final Set< UUID > blobIds )
        {
            final PersistenceTarget< ? > pt = 
                    m_serviceManager.getRetriever( m_persistenceTargetClass ).attain( m_persistenceTargetId );
            final UUID storageDomainId;
            if (null == pt.getStorageDomainMemberId() )
            {
                storageDomainId = null;
            }
            else
            {
                storageDomainId = m_serviceManager.getRetriever(StorageDomainMember.class )
                    .attain( pt.getStorageDomainMemberId() ).getStorageDomainId();
            }
            final Bucket bucket = 
                    m_serviceManager.getRetriever( Bucket.class ).attain( Require.exists( 
                            S3Object.class,
                            S3Object.BUCKET_ID,
                            Require.exists(
                                    Blob.class,
                                    Blob.OBJECT_ID,
                                    Require.beanPropertyEqualsOneOf( Identifiable.ID, blobIds ) ) ) );
            final DataPersistenceRule rule = 
                    m_serviceManager.getRetriever( DataPersistenceRule.class ).retrieve( Require.all( 
                            Require.beanPropertyEquals(
                                    DataPlacement.DATA_POLICY_ID, bucket.getDataPolicyId() ),
                            Require.beanPropertyEquals( 
                                    DataPersistenceRule.STORAGE_DOMAIN_ID, storageDomainId ) ) );
            if ( null == rule )
            {
                LOG.info( "No rebuild required for blob loss since no persistence rule applies." );
                return null;
            }
            if ( DataPersistenceRuleType.PERMANENT != rule.getType() )
            {
                LOG.info( "No rebuild required for blob loss since persistence rule is of type " 
                          + rule.getType() + "." );
                return null;
            }
            return rule;
        }
        
        
        @Override
        protected void populateDegradedBlob(
                final DegradedBlob degradedBlob,
                final DataPlacement< ? > dataPlacement )
        {
            degradedBlob.setPersistenceRuleId( dataPlacement.getId() );
        }
        

        @Override
        protected void commit( final Set< UUID > blobIds )
        {
            LOG.warn( "Blobs lost on " + m_persistenceTargetClass.getSimpleName() + " " 
                      + m_persistenceTargetId 
                      + " due to " + m_error + ": " + LogUtil.getShortVersion( blobIds, 10 ) );
            m_dataManager.deleteBeans( m_blobPersistenceTargetClass, Require.all( 
                    Require.beanPropertyEquals( 
                            m_persistenceTargetClass.getSimpleName().toLowerCase() + "Id", 
                            m_persistenceTargetId ),
                    Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ) );
        }
        
        
        private final Class< T > m_persistenceTargetClass;
        private final Class< BT > m_blobPersistenceTargetClass;
        private final UUID m_persistenceTargetId;
    } // end inner class def
    
    
    private final class TargetBlobLossRecorder
        < T extends DatabasePersistable & BlobTarget< T >, R extends Identifiable & DataReplicationRule< R > >
        extends BlobLossRecorder
    {
        private TargetBlobLossRecorder(
                final Class< T > blobTargetType,
                final Class< R > replicationRuleType,
                final String degradedBlobReplicationRulePropertyName,
                final UUID targetId,
                String error,
                final Set< UUID > blobIds )
        {
            super( error, blobIds );
            m_blobTargetType = blobTargetType;
            m_replicationRuleType = replicationRuleType;
            m_replicationRuleIdSetter =
                    BeanUtils.getWriter( DegradedBlob.class, degradedBlobReplicationRulePropertyName );
            m_targetId = targetId;
            
            Validations.verifyNotNull( "Replication rule id setter", m_replicationRuleIdSetter );
        }
        
        
        @Override
        protected DataPlacement< ? > getRuleToRebuild( final Set< UUID > blobIds )
        {
            final Bucket bucket = 
                    m_serviceManager.getRetriever( Bucket.class ).attain( Require.exists( 
                            S3Object.class,
                            S3Object.BUCKET_ID,
                            Require.exists(
                                    Blob.class,
                                    Blob.OBJECT_ID,
                                    Require.beanPropertyEqualsOneOf( Identifiable.ID, blobIds ) ) ) );
            final R rule = 
                    m_serviceManager.getRetriever( m_replicationRuleType ).retrieve( Require.all( 
                            Require.beanPropertyEquals(
                                    DataPlacement.DATA_POLICY_ID, bucket.getDataPolicyId() ),
                            Require.beanPropertyEquals( 
                                    BlobTarget.TARGET_ID, m_targetId ) ) );
            if ( null == rule )
            {
                LOG.info( "No rebuild required for blob loss since no replication rule applies." );
                return null;
            }
            if ( DataReplicationRuleType.PERMANENT != rule.getType() )
            {
                LOG.info( "No rebuild required for blob loss since replication rule is of type " 
                          + rule.getType() + "." );
                return null;
            }
            return rule;
        }
        
        
        @Override
        protected void populateDegradedBlob(
                final DegradedBlob degradedBlob,
                final DataPlacement< ? > dataPlacement )
        {
            try
            {
                m_replicationRuleIdSetter.invoke( degradedBlob, dataPlacement.getId() );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        

        @Override
        protected void commit( final Set< UUID > blobIds )
        {
            LOG.warn( blobIds.size() + " " + m_blobTargetType.getSimpleName() + "s lost on target "
                      + m_targetId + " due to " + m_error + ": " + LogUtil.getShortVersion( blobIds, 10 ) );
            m_dataManager.deleteBeans( m_blobTargetType, Require.all( 
                    Require.beanPropertyEquals( 
                            BlobTarget.TARGET_ID, 
                            m_targetId ),
                    Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ) );
        }
        
        
        private final Class< T > m_blobTargetType;
        private final Class< R > m_replicationRuleType;
        private final Method m_replicationRuleIdSetter;
        private final UUID m_targetId;
    } // end inner class def
}
