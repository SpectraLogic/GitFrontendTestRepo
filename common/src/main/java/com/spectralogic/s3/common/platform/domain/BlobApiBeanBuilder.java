/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.PhysicalPlacementApiBean;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;

public final class BlobApiBeanBuilder
{
    public BlobApiBeanBuilder( 
            final BeansRetriever< Bucket > bucketRetriever,
            final BeansRetriever< S3Object > objectRetriever,
            final Set< Blob > blobs )
    {
        this( bucketRetriever,
              BeanUtils.toMap( objectRetriever.retrieveAll( 
                BeanUtils.< UUID >extractPropertyValues( blobs, Blob.OBJECT_ID ) ).toSet() ),
              blobs );
    }
    
    
    private BlobApiBeanBuilder(
            final BeansRetriever< Bucket > bucketRetriever,
            final Map< UUID, S3Object > objects, 
            final Set< Blob > blobs )
    {
        m_blobs = new HashSet<>( blobs );
        m_objects = objects;
        
        m_buckets = new HashMap<>();
        if ( null != bucketRetriever )
        {
            for ( final Bucket bucket : bucketRetriever.retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues(
                                objects.values(), S3Object.BUCKET_ID ) ).toSet() )
            {
                m_buckets.put( bucket.getId(), bucket.getName() );
            }
        }
        
        m_blobsWithoutPhysicalPlacement = BeanUtils.toMap( m_blobs ).keySet();
    }
    
    
    public BlobApiBeanBuilder includeBlobCacheState( final Set< UUID > blobIdsInCache )
    {
        Validations.verifyNotNull( "Blob ids in cache", blobIdsInCache );
        m_blobIdsInCache = blobIdsInCache;
        return this;
    }
    
    
    public BlobApiBeanBuilder includePhysicalPlacement( 
            final BeansRetrieverManager brm,
            final boolean onlyIncludeSuspectPhysicalPlacement )
    {
        Validations.verifyNotNull( "Beans retriever manager", brm );
        m_brm = brm;
        m_onlyIncludeSuspectPhysicalPlacement = onlyIncludeSuspectPhysicalPlacement;
        return this;
    }
    
    
    public BlobApiBeanBuilder filterToStorageDomain( final UUID storageDomainId )
    {
        m_storageDomainId = storageDomainId;
        return this;
    }
    
    
    public BlobApiBean [] build()
    {
        final Map< UUID, PhysicalPlacementApiBean > physicalPlacements = calculatePhysicalPlacements();
        final Set< BlobApiBean > retval = new HashSet<>();
        for ( final Blob blob : m_blobs )
        {
            final S3Object o = m_objects.get( blob.getObjectId() );
            if ( null == o ) 
            { 
                LOG.warn( "Could not find object for blob " + blob.getId() 
                          + " (the blob and object have likely both been deleted)." ); 
                continue; 
            } 
            retval.add( BeanFactory.newBean( BlobApiBean.class )
                    .setId( blob.getId() )
                    .setOffset( blob.getByteOffset() )
                    .setLength( blob.getLength() )
                    .setInCache( ( null == m_blobIdsInCache ) ? 
                            null 
                            : Boolean.valueOf( m_blobIdsInCache.contains( blob.getId() ) ) )
                    .setName( o.getName() )
                    .setBucket( m_buckets.get( o.getBucketId() ) )
                    .setLatest( o.isLatest() )
                    .setVersionId( o.getId() )
                    .setPhysicalPlacement( physicalPlacements.get( blob.getId() ) ) );
        }
            
        return CollectionFactory.toArray( BlobApiBean.class, BeanUtils.sort( retval ) );
    }
    
    
    public BlobApiBeansContainer buildAndWrap()
    {
        final BlobApiBeansContainer retval = BeanFactory.newBean( BlobApiBeansContainer.class );
        retval.setObjects( build() );
        return retval;
    }
    
    
    public Set< UUID > getBlobsWithoutPhysicalPlacement()
    {
        return new HashSet<>( m_blobsWithoutPhysicalPlacement );
    }
    
    
    private Map< UUID, PhysicalPlacementApiBean > calculatePhysicalPlacements()
    {
        if ( null == m_brm )
        {
            return new HashMap<>();
        }
        
        final Set< UUID > blobIds = BeanUtils.toMap( m_blobs ).keySet();
        final Map< UUID, PhysicalPlacementApiBean > retval = new HashMap<>();
        for ( final UUID id : blobIds )
        {
            retval.put( 
                    id, 
                    BeanFactory.newBean( PhysicalPlacementApiBean.class ) );
        }
        
        populateTapePlacement( retval, blobIds );
        populatePoolPlacement( retval, blobIds );
        populateTargetPlacement( 
                BlobDs3Target.class, SuspectBlobDs3Target.class, Ds3Target.class,
                Ds3Target.ADMIN_SECRET_KEY, retval, blobIds );
        populateTargetPlacement( 
                BlobAzureTarget.class, SuspectBlobAzureTarget.class, AzureTarget.class,
                AzureTarget.ACCOUNT_KEY, retval, blobIds );
        populateTargetPlacement( 
                BlobS3Target.class, SuspectBlobS3Target.class, S3Target.class,
                S3Target.SECRET_KEY, retval, blobIds );

        return retval;
    }
    
    
    private void populateTapePlacement( 
            final Map< UUID, PhysicalPlacementApiBean > retval,
            final Set< UUID > blobIds )
    {
        final BeansRetriever< ? extends BlobTape > retriever = ( m_onlyIncludeSuspectPhysicalPlacement ) ?
                m_brm.getRetriever( SuspectBlobTape.class )
                : m_brm.getRetriever( BlobTape.class );
        final Set< ? extends BlobTape > blobTapes = retriever.retrieveAll( Require.beanPropertyEqualsOneOf(
                BlobObservable.BLOB_ID, blobIds ) ).toSet();
        final Map< UUID, Tape > tapes = BeanUtils.toMap( 
                m_brm.getRetriever( Tape.class ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues( blobTapes, BlobTape.TAPE_ID ) ).toSet() );
        for ( final BlobTape bt : blobTapes )
        {
            m_blobsWithoutPhysicalPlacement.remove( bt.getBlobId() );
            final PhysicalPlacementApiBean pp = retval.get( bt.getBlobId() );
            final Tape tape = tapes.get( bt.getTapeId() );
            if ( !pp.tapesList().contains( tape ) )
            {
                if ( null == m_storageDomainId || m_storageDomainId.equals(
                        m_brm.getRetriever( StorageDomainMember.class )
                        .retrieve( tape.getStorageDomainMemberId() ).getStorageDomainId() ) )
                {
                    pp.tapesList().add( tape );
                    Collections.sort( pp.tapesList(), new BeanComparator<>( Tape.class, Tape.BAR_CODE ) );
                }
            }
        }
    }
    
    
    private void populatePoolPlacement( 
            final Map< UUID, PhysicalPlacementApiBean > retval,
            final Set< UUID > blobIds )
    {
        final BeansRetriever< ? extends BlobPool > retriever = ( m_onlyIncludeSuspectPhysicalPlacement ) ?
                m_brm.getRetriever( SuspectBlobPool.class )
                : m_brm.getRetriever( BlobPool.class );
        final Set< ? extends BlobPool > blobPools = retriever.retrieveAll( Require.beanPropertyEqualsOneOf(
                BlobObservable.BLOB_ID, blobIds ) ).toSet();
        final Map< UUID, Pool > pools = BeanUtils.toMap( 
                m_brm.getRetriever( Pool.class ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues( blobPools, BlobPool.POOL_ID ) ).toSet() );
        for ( final BlobPool bp : blobPools )
        {
            m_blobsWithoutPhysicalPlacement.remove( bp.getBlobId() );
            final PhysicalPlacementApiBean pp = retval.get( bp.getBlobId() );
            final Pool pool = pools.get( bp.getPoolId() );
            if ( !pp.poolsList().contains( pool ) )
            {
                if ( null == m_storageDomainId || m_storageDomainId.equals(
                        m_brm.getRetriever( StorageDomainMember.class )
                        .retrieve( pool.getStorageDomainMemberId() ).getStorageDomainId() ) )
                {
                    pp.poolsList().add( pool );
                    Collections.sort( pp.poolsList(), new BeanComparator<>( Pool.class, NameObservable.NAME ) );
                }
                
            }
        }
    }
    
    
    private < BT extends BlobTarget< BT > & DatabasePersistable,
               T extends ReplicationTarget< T > & DatabasePersistable >
    void populateTargetPlacement( 
            final Class< BT > blobTargetType,
            final Class< ? extends BT > suspectBlobTargetType,
            final Class< T > targetType,
            final String secretProperty,
            final Map< UUID, PhysicalPlacementApiBean > retval,
            final Set< UUID > blobIds )
    {
        final BeansRetriever< ? extends BT > retriever =
                ( m_onlyIncludeSuspectPhysicalPlacement ) ?
                        m_brm.getRetriever( suspectBlobTargetType )
                        : m_brm.getRetriever( blobTargetType );
        final Set< ? extends BT > blobTargets =
                retriever.retrieveAll( Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blobIds ) ).toSet();
        final Map< UUID, T > targets = BeanUtils.toMap( 
                m_brm.getRetriever( targetType ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues(
                                blobTargets, BlobTarget.TARGET_ID ) ).toSet() );
        for ( final BT bt : blobTargets )
        {
            m_blobsWithoutPhysicalPlacement.remove( bt.getBlobId() );
            final PhysicalPlacementApiBean pp = retval.get( bt.getBlobId() );
            final List< T > targetList = pp.targetList( targetType );
            if ( !targetList.contains( targets.get( bt.getTargetId() ) ) )
            {
                if ( null != m_storageDomainId )
                {
                    continue;
                }
                targetList.add( targets.get( bt.getTargetId() ) );
                Collections.sort( 
                        targetList, 
                        new BeanComparator<>( targetType, NameObservable.NAME ) );
            }
            for ( final T target : targetList )
            {
                final Method writer = BeanUtils.getWriter( targetType, secretProperty );
                try
                {
                    writer.invoke( target, LogUtil.CONCEALED );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
    }
    
    
    private volatile UUID m_storageDomainId;
    private volatile Set< UUID > m_blobIdsInCache;
    private volatile BeansRetrieverManager m_brm;
    private volatile boolean m_onlyIncludeSuspectPhysicalPlacement;
    
    private final Map< UUID, S3Object > m_objects;
    private final Map< UUID, String > m_buckets;
    private final Set< Blob > m_blobs;
    private final Set< UUID > m_blobsWithoutPhysicalPlacement;
    private static final Logger LOG = Logger.getLogger( BlobApiBeanBuilder.class );
}
