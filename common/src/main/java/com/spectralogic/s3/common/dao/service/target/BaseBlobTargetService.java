/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.service.ds3.DegradedBlobService;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.lang.Validations;

abstract class BaseBlobTargetService
    < T extends DatabasePersistable & BlobTarget< T >, 
      R extends DatabasePersistable & DataReplicationRule< R > >
    extends BaseService< T > implements BlobTargetService< T, R >
{
    protected BaseBlobTargetService(
            final Class< T > clazz,
            final Class< R > replicationRuleType,
            final String degradedBlobReplicationRulePropertyName )
    {
        super( clazz );
        m_replicationRuleType = replicationRuleType;
        m_degradedBlobReplicationRulePropertyName = degradedBlobReplicationRulePropertyName;
    }
    
    
    public void blobsSuspect( final String error, final Set< T > blobTargets )
    {
        UUID targetId = null;
        for ( final T bt : blobTargets )
        {
            if ( null == targetId )
            {
                targetId = bt.getTargetId();
            }
            else if ( !targetId.equals( bt.getTargetId() ) )
            {
                throw new IllegalArgumentException( 
                        "Cannot call method with blob targets spanning different targets." );
            }
        }
        
        blobsLost( 
                error, 
                targetId, 
                BeanUtils.< UUID >extractPropertyValues( blobTargets, BlobObservable.BLOB_ID ) );
    }


    public void blobsLost( 
            final String error,
            final UUID targetId, 
            final Set< UUID > blobIds )
    {
        getServiceManager().getService( DegradedBlobService.class ).blobsLostOnTarget( 
                getServicedType(),
                m_replicationRuleType,
                m_degradedBlobReplicationRulePropertyName,
                targetId,
                error, 
                blobIds );
    }


    public void reclaimForDeletedReplicationRule( 
            final UUID dataPolicyId, 
            final R rule )
    {
        Validations.verifyNotNull( "Target id", rule.getTargetId() );
        final Set< UUID > affectedBuckets = BeanUtils.extractPropertyValues( 
                getServiceManager().getRetriever( Bucket.class ).retrieveAll( 
                        Bucket.DATA_POLICY_ID, dataPolicyId ).toSet(),
                Identifiable.ID );
        
        getDataManager().deleteBeans( getServicedType(), Require.all( 
                Require.beanPropertyEquals( BlobTarget.TARGET_ID, rule.getTargetId() ),
                Require.exists( 
                        BlobObservable.BLOB_ID, 
                        Require.exists( 
                                Blob.OBJECT_ID,
                                Require.beanPropertyEqualsOneOf(
                                        S3Object.BUCKET_ID, 
                                        affectedBuckets ) ) ) ) );
    }
    
    
    private final Class< R > m_replicationRuleType;
    private final String m_degradedBlobReplicationRulePropertyName;
}
