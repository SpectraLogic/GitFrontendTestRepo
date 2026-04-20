/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;


import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.util.db.query.Require;
import lombok.NonNull;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.target.DataOfflineablePublicCloudReplicationTarget;
import com.spectralogic.s3.dataplanner.backend.target.api.OfflineDataStagingWindowManager;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.render.BytesRenderer;

public final class OfflineDataStagingWindowManagerImpl implements OfflineDataStagingWindowManager
{
    public OfflineDataStagingWindowManagerImpl(
            @NonNull final BeansServiceManager serviceManager,
            final int refreshFrequencyInMillis )
    {
        m_serviceManager = serviceManager;
        m_refreshFrequencyInMillis = refreshFrequencyInMillis;
    }
    
    
    synchronized public < T extends DatabasePersistable > 
    boolean tryLock( @NonNull final Class< T > targetType,
                     @NonNull final UUID targetId,
                     @NonNull final UUID blobId )
    {
        if ( !m_locks.containsKey( targetId ) )
        {
            m_locks.put( targetId, new HashMap< UUID, Long >() );
        }
        
        final Map< UUID, Long > locks = m_locks.get( targetId );
        if ( locks.containsKey( blobId ) )
        {
            return true;
        }
        
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final long windowLimit = 
                ( ( DataOfflineablePublicCloudReplicationTarget< ? >)
                m_serviceManager.getRetriever( targetType ).attain( targetId ) )
                .getOfflineDataStagingWindowInTb() * 1024 * 1024L * 1024 * 1024;
        final long bytesRequired =
                m_serviceManager.getService( BlobService.class ).getSizeInBytes(Collections.singleton(blobId));
        long bytesLocked = getBytesLocked( targetId );
        if ( bytesLocked + bytesRequired > windowLimit )
        {
            refreshIfNecessary();
            bytesLocked = getBytesLocked( targetId );
        }
        if ( bytesLocked + bytesRequired > windowLimit )
        {
            LOG.info( "Cannot lock " + bytesRenderer.render( bytesRequired )
                      + " for staging offline data in blob " + blobId + " ("
                      + bytesRenderer.render( bytesLocked ) + " of " 
                      + bytesRenderer.render( windowLimit ) + " locked)." );
            return false;
        }
        
        locks.put( blobId, Long.valueOf( bytesRequired ) );
        LOG.info( "Locked " + bytesRenderer.render( bytesRequired )
                + " for staging offline data in blob " + blobId + " ("
                + bytesRenderer.render( bytesLocked + bytesRequired ) + " of " 
                + bytesRenderer.render( windowLimit ) + " locked)." );
        return true;
    }
    
    
    private long getBytesLocked( final UUID targetId )
    {
        long retval = 0;
        for ( final Long lock : m_locks.get( targetId ).values() )
        {
            retval += lock.longValue();
        }
        
        return retval;
    }
    
    
    private void refreshIfNecessary()
    {
        if ( m_durationSinceRefreshed.getElapsedMillis() < m_refreshFrequencyInMillis )
        {
            return;
        }
        
        removeDeadChunks();
        m_durationSinceRefreshed.reset();
    }
    
    
    private void removeDeadChunks()
    {
        final Set< UUID > blobIds = new HashSet<>();
        for ( final Map.Entry< UUID, Map< UUID, Long > > e : m_locks.entrySet() )
        {
            blobIds.addAll( e.getValue().keySet() );
        }

        final Set< UUID > activeBlobIds = new HashSet<>();
        for (UUID blobId : blobIds) {
            Set<JobEntry> jobEntries = m_serviceManager.getRetriever(JobEntry.class).retrieveAll(
                    Require.all(
                            Require.beanPropertyEqualsOneOf(BlobObservable.BLOB_ID, blobIds))).toSet();
            if (!jobEntries.isEmpty()) {
                activeBlobIds.add(blobId);
            }
        }

        blobIds.removeAll(activeBlobIds);
        if ( !blobIds.isEmpty() )
        {
            LOG.info( blobIds.size() + " blobs are dead and can have their locks released." );
        }
        for ( final UUID deadChunkId : blobIds )
        {
            releaseLock( deadChunkId );
        }
    }
    
    
    synchronized public void releaseLock( final UUID chunkId )
    {
        Validations.verifyNotNull( "Chunk id", chunkId );
        
        final BytesRenderer bytesRenderer = new BytesRenderer();
        for ( final Map.Entry< UUID, Map< UUID, Long > > e : m_locks.entrySet() )
        {
            if ( e.getValue().containsKey( chunkId ) )
            {
                final Long bytes = e.getValue().remove( chunkId );
                LOG.info( "Released lock of " + bytesRenderer.render( bytes )
                          + " for staging offline data in chunk " + chunkId + "." );
            }
        }
    }
    
    
    /** Map< targetid, Map< chunkid, #bytes > > */
    private final Map< UUID, Map< UUID, Long > > m_locks = new HashMap<>();
    private final BeansServiceManager m_serviceManager;
    private final Duration m_durationSinceRefreshed = new Duration();
    private final int m_refreshFrequencyInMillis;
    
    private final static Logger LOG = Logger.getLogger( OfflineDataStagingWindowManagerImpl.class );
}
