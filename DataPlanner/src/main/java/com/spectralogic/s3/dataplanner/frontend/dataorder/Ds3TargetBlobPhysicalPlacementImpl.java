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

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3TargetReadPreference;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreference;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;

public final class Ds3TargetBlobPhysicalPlacementImpl implements Ds3TargetBlobPhysicalPlacement
{
    public Ds3TargetBlobPhysicalPlacementImpl(
            final Set< UUID > blobIds,
            final BeansRetrieverManager brm,
            final Ds3ConnectionFactory ds3ConnectionFactory )
    {
        if ( null == blobIds || blobIds.isEmpty() )
        {
            throw new IllegalArgumentException( "Blob ids cannot be empty." );
        }
        initializeTargetPlacement( blobIds, brm, ds3ConnectionFactory );
    }


    private void initializeTargetPlacement(
            final Set< UUID > blobIds,
            final BeansRetrieverManager brm,
            final Ds3ConnectionFactory ds3ConnectionFactory)
    {
        for ( final Ds3Target target : getTargets( brm.getRetriever( Ds3Target.class ) ) )
        {
            try
            {
                final Ds3Connection connection = ds3ConnectionFactory.connect( null, target );
                try
                {
                    final Set< UUID > blobIdsOnTarget = BeanUtils.extractPropertyValues(
                            brm.getRetriever( BlobDs3Target.class ).retrieveAll( Require.all(
                                    Require.beanPropertyEquals(
                                            BlobTarget.TARGET_ID, target.getId() ),
                                    Require.beanPropertyEqualsOneOf(
                                            BlobObservable.BLOB_ID, blobIds ),
                                    Require.not( Require.exists(
                                            SuspectBlobDs3Target.class,
                                            Identifiable.ID,
                                            Require.nothing() ) ) ) ).toSet(),
                            BlobObservable.BLOB_ID );
                    if ( !blobIdsOnTarget.isEmpty() )
                    {
                        processBlobPersistenceResponse(
                                target,
                                connection.getBlobPersistence( null, blobIdsOnTarget ) );
                    }
                }
                catch ( final RuntimeException ex )
                {
                    LOG.info( "Failed to get blob persistence information on target "
                              + getTargetDescription( target ) + ".", ex );
                }
                finally
                {
                    connection.shutdown();
                }
            }
            catch ( final RuntimeException ex )
            {
                LOG.info( "Cannot connect to target " + getTargetDescription( target ) + ".", ex );
            }
        }

        computeTargetReadPreferences( blobIds.iterator().next(), brm );
    }
    
    private Set< Ds3Target > getTargets( final BeansRetriever< Ds3Target > retriever )
    {
        final Set< Ds3Target > retval = new HashSet<>();
        for ( final Ds3Target target : retriever.retrieveAll().toSet() )
        {
            if ( TargetState.ONLINE != target.getState() )
            {
                LOG.info( "Will not consider target " + getTargetDescription( target ) + " since it is in " 
                          + ReplicationTarget.STATE + " " + target.getState() + "." );
                continue;
            }
            if ( Quiesced.NO != target.getQuiesced() )
            {
                LOG.info( "Will not consider target " + getTargetDescription( target ) + " since it is " 
                          + ReplicationTarget.QUIESCED + "=" + target.getQuiesced() + "." );
                continue;
            }
            
            retval.add( target );
        }
        
        return retval;
    }
    
    
    private void processBlobPersistenceResponse( 
            final Ds3Target target, 
            final BlobPersistenceContainer response )
    {
        int tapeCount = 0;
        int poolCount = 0;
        final UUID targetId = target.getId();
        for ( final BlobPersistence blob : response.getBlobs() )
        {
            if ( blob.isAvailableOnTapeNow() )
            {
                ++tapeCount;
                if ( !m_blobsOnTape.containsKey( targetId ) )
                {
                    m_blobsOnTape.put( targetId, new HashSet< UUID >() );
                }
                m_blobsOnTape.get( targetId ).add( blob.getId() );
            }
            if ( blob.isAvailableOnPoolNow() )
            {
                ++poolCount;
                if ( !m_blobsOnPool.containsKey( targetId ) )
                {
                    m_blobsOnPool.put( targetId, new HashSet< UUID >() );
                }
                m_blobsOnPool.get( targetId ).add( blob.getId() );
            }
        }
        
        if ( 0 < tapeCount + poolCount )
        {
            LOG.info( "Will consider target " + getTargetDescription( target ) + " for its " + tapeCount 
                      + " blobs on tape and " + poolCount + " blobs on pool." );
        }
    }
    
    
    private void computeTargetReadPreferences( final UUID blobId, final BeansRetrieverManager brm )
    {
        final Blob blob = brm.getRetriever( Blob.class ).attain( blobId );
        final S3Object o = brm.getRetriever( S3Object.class ).attain( blob.getObjectId() );
        
        final Set< UUID > targetIds = new HashSet<>( m_blobsOnPool.keySet() );
        targetIds.addAll( m_blobsOnTape.keySet() );
        for ( final Ds3Target target : brm.getRetriever( Ds3Target.class ).retrieveAll( targetIds ).toSet() )
        {
            final Ds3TargetReadPreference customReadPreference =
                    brm.getRetriever( Ds3TargetReadPreference.class ).retrieve( Require.all( 
                            Require.beanPropertyEquals( 
                                    TargetReadPreference.BUCKET_ID, o.getBucketId() ),
                            Require.beanPropertyEquals( 
                                    TargetReadPreference.TARGET_ID, target.getId() ) ) );
            m_candidateTargets.put( target.getId(), ( null == customReadPreference ) ? 
                    target.getDefaultReadPreference() 
                    : customReadPreference.getReadPreference() );
        }
    }
    
    
    public Set< UUID > getCandidateTargets()
    {
        return new HashSet<>( m_candidateTargets.keySet() );
    }
   
    
    public TargetReadPreferenceType getReadPreference( final UUID targetId )
    {
        return m_candidateTargets.get( targetId );
    }
    
    
    public Set< UUID > getBlobsOnTape( final UUID targetId )
    {
        final Set< UUID > retval = m_blobsOnTape.get( targetId );
        return ( null == retval ) ? new HashSet< UUID >() : new HashSet<>( retval );
    }
    
    
    public Set< UUID > getBlobsOnPool( final UUID targetId )
    {
        final Set< UUID > retval = m_blobsOnPool.get( targetId );
        return ( null == retval ) ? new HashSet< UUID >() : new HashSet<>( retval );
    }
    
    
    private String getTargetDescription( final Ds3Target target )
    {
        return target.getId() + " (" + target.getName() + ")";
    }
    
    
    private final Map< UUID, TargetReadPreferenceType > m_candidateTargets = new HashMap<>();
    private final Map< UUID, Set< UUID > > m_blobsOnTape = new HashMap<>();
    private final Map< UUID, Set< UUID > > m_blobsOnPool = new HashMap<>();
    private final static Logger LOG = Logger.getLogger( Ds3TargetBlobPhysicalPlacementImpl.class );
}
