/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.dataorder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreference;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

public final class PublicCloudBlobSupport
    < T extends DatabasePersistable & PublicCloudReplicationTarget< T >, 
      BT extends DatabasePersistable & BlobTarget< BT >,
      RP extends DatabasePersistable & TargetReadPreference< RP > >
{
    public PublicCloudBlobSupport(
            final Class<T> targetType,
            final Class<BT> blobTargetType,
            final Class<RP> targetReadPreferenceType,
            final Set<UUID> blobIds,
            final BeansServiceManager serviceManager)
    {
        Validations.verifyNotNull( "Target type", targetType );
        Validations.verifyNotNull( "Blob target type", blobTargetType );
        Validations.verifyNotNull( "Target read preference type", targetReadPreferenceType );
        Validations.verifyNotEmptyCollection( "Blob ids", blobIds );
        Validations.verifyNotNull( "Service manager", serviceManager );
        
        final Blob blob = serviceManager.getRetriever( Blob.class ).attain( blobIds.iterator().next() );
        final S3Object object = serviceManager.getRetriever( S3Object.class ).attain( blob.getObjectId() );
        final UUID bucketId = object.getBucketId();
        for ( final T target : serviceManager.getRetriever( targetType ).retrieveAll().toSet() )
        {
            if ( TargetState.ONLINE != target.getState() )
            {
                continue;
            }
            
            final Set< BT > bts = serviceManager.getRetriever( blobTargetType ).retrieveAll( Require.all( 
                    Require.beanPropertyEquals( BlobTarget.TARGET_ID, target.getId() ),
                    Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ) ).toSet();
            if ( !bts.isEmpty() )
            {
                m_blobTargets.put( 
                        target.getId(),
                        BeanUtils.extractPropertyValues( bts, BlobObservable.BLOB_ID ) );
                
                final RP readPreference = serviceManager.getRetriever( targetReadPreferenceType ).retrieve( 
                        Require.all( 
                                Require.beanPropertyEquals( TargetReadPreference.BUCKET_ID, bucketId ),
                                Require.beanPropertyEquals( 
                                        TargetReadPreference.TARGET_ID, target.getId() ) ) );
                m_targetReadPreferences.put(
                        target.getId(),
                        ( null == readPreference ) ? 
                                target.getDefaultReadPreference()
                                : readPreference.getReadPreference() );
            }
        }
    }
    
    
    public Map< UUID, Set< UUID > > getBlobs(final TargetReadPreferenceType readPreferenceRequired)
    {
        Validations.verifyNotNull( "Read preference required", readPreferenceRequired );
        
        final Map< UUID, Set< UUID > > retval = new HashMap<>();
        for ( final Map.Entry< UUID, TargetReadPreferenceType > targetReadPreferenceEntry : m_targetReadPreferences.entrySet() )
        {
            if ( readPreferenceRequired == targetReadPreferenceEntry.getValue() )
            {
                retval.put( targetReadPreferenceEntry.getKey(), new HashSet<>( m_blobTargets.get( targetReadPreferenceEntry.getKey() ) ) );
            }
        }
        
        return retval;
    }
    
    
    private final Map< UUID, Set< UUID > > m_blobTargets = new HashMap<>();
    private final Map< UUID, TargetReadPreferenceType > m_targetReadPreferences = new HashMap<>();
}
