/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape;

import java.lang.reflect.Field;

import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.RpcServerPort;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeBlobStoreProcessorImpl;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeEnvironmentImpl;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapePartitionMoveFailureSupport;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.client.RpcClientImpl;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public final class TapeBlobStoreTestSupport extends BaseShutdownable
{
    public TapeBlobStoreTestSupport( final DatabaseSupport dbSupport )
    {
        m_serviceManager = dbSupport.getServiceManager();
        m_mockCacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        m_cacheManager = new DiskManagerImpl( new CacheManagerImpl( m_serviceManager,
                new MockTierExistingCacheImpl() ) );
        m_blobStore = new TapeBlobStoreImpl(
                new RpcClientImpl( "localhost", RpcServerPort.TAPE_BACKEND ), 
                m_cacheManager, 
                new JobProgressManagerImpl( m_serviceManager, BufferProgressUpdates.NO ),
                m_serviceManager );
        m_processor = getFieldValue(
                TapeBlobStoreImpl.class, m_blobStore, TapeBlobStoreImpl.FIELD_PROCESSOR );
        m_environment = getFieldValue(
                TapeBlobStoreImpl.class, m_blobStore, TapeBlobStoreImpl.FIELD_ENVIRONMENT );
        m_moveFailureSupport = getFieldValue( 
                TapeEnvironmentImpl.class, m_environment, "m_moveFailureSupport" );
        m_periodicNewTaskRunner = getFieldValue( 
                TapeBlobStoreProcessorImpl.class, m_processor, "m_tapeTaskStarter" );
        m_taskSchedulingListener = getFieldValue(
                TapeBlobStoreProcessorImpl.class, m_processor, "m_tapeTaskStarter" );
        m_moveFailureSupport.setSuspensionInMillisUponMoveFailure( 25 );
        
        addShutdownListener( m_blobStore );
        addShutdownListener( m_mockCacheFilesystemDriver );
    }
    
    
    private < T > T getFieldValue( final Class< ? > clazz, final Object instance, final String fieldName )
    {
        try
        {
            final Field f = clazz.getDeclaredField( fieldName );
            f.setAccessible( true );
            @SuppressWarnings( "unchecked" )
            final T retval = (T)f.get( instance );
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    public DiskManager getCacheManager()
    {
        return m_cacheManager;
    }
    
    
    public MockCacheFilesystemDriver getMockCacheFilesystemDriver()
    {
        return m_mockCacheFilesystemDriver;
    }
    
    
    public TapeBlobStoreProcessor getProcessor()
    {
        return m_processor;
    }
    
    
    public TapeBlobStore getBlobStore()
    {
        return m_blobStore;
    }
    
    
    public void mockPeriodicStartNewTasksCall()
    {
        m_periodicNewTaskRunner.run();
    }
    
    
    public void mockTaskSchedulingRequiredCall()
    {
        m_taskSchedulingListener.taskSchedulingRequired( null );
    }
    

    private final BeansServiceManager m_serviceManager;
    private final DiskManager m_cacheManager;
    private final TapePartitionMoveFailureSupport m_moveFailureSupport;
    private final MockCacheFilesystemDriver m_mockCacheFilesystemDriver;
    private final TapeBlobStore m_blobStore;
    private final TapeEnvironment m_environment;
    private final TapeBlobStoreProcessor m_processor;
    private final Runnable m_periodicNewTaskRunner;
    private final BlobStoreTaskSchedulingListener m_taskSchedulingListener;
}
