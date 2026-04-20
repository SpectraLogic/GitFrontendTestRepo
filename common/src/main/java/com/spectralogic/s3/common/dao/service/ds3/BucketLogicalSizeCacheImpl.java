/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.ThreadedInitializable;
import com.spectralogic.util.thread.wp.DBParallelQueryPool;
import com.spectralogic.util.thread.wp.WorkPool;

/**
 * Caches information about the amount of information stored in each bucket.
 */
final class BucketLogicalSizeCacheImpl extends ThreadedInitializable implements BucketLogicalSizeCache
{
    BucketLogicalSizeCacheImpl( final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Beans retriever manager", serviceManager );
        m_serviceManager = serviceManager;
        startInitialization();
    }    
    
    
    @Override
    protected Set< Runnable > getInitializers()
    {
        final Set< Bucket > buckets = m_serviceManager.getRetriever( Bucket.class ).retrieveAll().toSet();
        return CollectionFactory.toSet( new BucketInitializer( BeanUtils.toMap( buckets )
                                                                        .keySet() ) );
    }
    
    
    public long getSize( final UUID bucketId )
    {
        if ( !m_initialized )
        {
            return -1;
        }
    
        synchronized ( m_lock )
        {
            if ( null == bucketId )
            {
                long retval = 0;
                for ( final Long v : m_buckets.values() )
                {
                    retval += v;
                }
                return retval;
            }
        
            final Long retval = m_buckets.get( bucketId );
            return ( null == retval ) ? 0 : retval;
        }
    }
    
    
    public void blobCreated( final UUID bucketId, final long sizeInBytes )
    {
        update( bucketId, sizeInBytes );
    }
    
    
    public void blobDeleted( final UUID bucketId, final long sizeInBytes )
    {
        update( bucketId, -sizeInBytes );
    }
    
    
    private void update( final UUID bucketId, final long sizeInBytes )
    {
        Validations.verifyNotNull( "Bucket id", bucketId );
        synchronized ( m_lock )
        {
            final Map< UUID, Long > map = ( m_initialized ) ? m_buckets : m_pendingChanges;
            Long originalSize = map.get( bucketId );
            if ( null == originalSize )
            {
                // an explicit null in the map means it's been deleted
                if ( map.containsKey( bucketId ) )
                {
                    return;
                }
                originalSize = 0L;
            }
            map.put( bucketId, sizeInBytes + originalSize );
        }
    }
    
    
    public void bucketDeleted( final UUID bucketId )
    {
        Validations.verifyNotNull( "Bucket id", bucketId );
        synchronized ( m_lock )
        {
            if ( m_initialized )
            {
                m_buckets.remove( bucketId );
            }
            else
            {
                m_pendingChanges.put( bucketId, null );
            }
        }
    }
    
    
    private final class BucketInitializer implements Runnable
    {
        private BucketInitializer( final Collection< UUID > bucketIds )
        {
            m_bucketIds = new HashSet<>( bucketIds );
        }
    
    
        private void processPendingChanges()
        {
            for ( final Map.Entry< UUID, Long > e : m_pendingChanges.entrySet() )
            {
                if ( null == e.getValue() )
                {
                    continue;
                }
                m_buckets.put( e.getKey(), e.getValue() );
            }
        }
    
    
        public void run()
        {
            try
            {
                final AtomicInteger bucketNumber = new AtomicInteger( 0 );
                final Set< Future< ? > > futures = new HashSet<>();
                final WorkPool workPool = DBParallelQueryPool.getInstance();
            
                m_bucketIds.forEach( bucketId -> futures.add( workPool.submit( () -> {
                    bucketNumber.incrementAndGet();
                    LOG.info(
                            "Loading metadata cache for bucket " + bucketNumber + " of " + m_bucketIds.size() + "..." );
                    update( bucketId, m_serviceManager.getService( BucketService.class )
                                                      .getLogicalCapacity( bucketId ) );
                } ) ) );
            
                futures.forEach( x -> {
                    try
                    {
                        x.get();
                    }
                    catch ( Exception e )
                    {
                        LOG.error( "Failed to initialize metadata cache.  The cache will be unusable.", e );
                        return;
                    }
                } );
            
                LOG.info( "Finished initializing metadata cache for " + m_bucketIds.size() +
                        " buckets.  The cache will be usable." );
            
                synchronized ( m_lock )
                {
                    m_initialized = true;
                    processPendingChanges();
                    m_pendingChanges.clear();
                }
            }
            catch ( final Exception ex )
            {
                LOG.error( "Failed to initialize metadata cache.  The cache will be unusable.", ex );
            }
        }
    
    
        private final Set< UUID > m_bucketIds;
    } // end inner class def
    
    
    private volatile boolean m_initialized;
    private final Map< UUID, Long > m_pendingChanges = new HashMap<>();
    private final Map< UUID, Long > m_buckets = new HashMap<>();
    private final Object m_lock = new Object();
    private final BeansServiceManager m_serviceManager;
    
    private final static Logger LOG = Logger.getLogger( BucketLogicalSizeCacheImpl.class );
}
