/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobDs3TargetService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.backend.target.task.MockDs3ConnectionFactory.PutBlobRequest;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.s3.dataplanner.testfrmwrk.TargetTaskBuilder;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public final class WriteChunkToDs3TargetTask_Test
{
    @Test
    public void testHappyConstruction()
    {
        final TargetWriteDirective<Ds3Target, Ds3BlobDestination> twd = new TargetWriteDirective<>(
                Ds3Target.class,
                new HashSet<Ds3BlobDestination>(),
                BeanFactory.newBean(Ds3Target.class),
                BlobStoreTaskPriority.values()[ 0 ],
                new HashSet<>(),
                0,
                BeanFactory.newBean(Bucket.class) );
        new WriteChunkToDs3TargetTask(
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), InterfaceProxyFactory.getProxy( DiskManager.class, null ), InterfaceProxyFactory.getProxy( BeansServiceManager.class, null ), InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ), twd
        );
    }
    

   @Test
    public void testPrepareForExecutionSelectsNoDs3TargetIfSelectionNotPossibleDueToState()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.updateBean( target.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunk(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunk.getId());
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        CollectionFactory.toSet( chunk ),
                        blob.getLength(),
                        bucket )
                );
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready to run to retry.");
    }
    

   @Test
    public void testPrepareForExecutionSelectsNoDs3TargetIfSelectionNotPossibleDueToQuiesced()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.updateBean( target.setQuiesced( Quiesced.PENDING ), ReplicationTarget.QUIESCED );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunk(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunk.getId());
        final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        CollectionFactory.toSet( chunk ),
                        blob.getLength(),
                        bucket )
        );
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready to run to retry.");
    }
    

   @Test
    public void testPrepareForExecutionSelectsNoDs3TargetIfSelectionNotPossibleDueToFailureToConnect()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );


        final MockDs3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory();
        ds3ConnectionFactory.setConnectException( new RuntimeException( "Can't connect." ) );

        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunk(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunk.getId());
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).withDs3ConnectionFactory(ds3ConnectionFactory).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        CollectionFactory.toSet( chunk ),
                        blob.getLength(),
                        bucket )
                );

        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready to run to retry.");
    }
    

   @Test
    public void testPrepareForExecutionWhenJobChunkNoLongerExistsNotPossible()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunk(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunk.getId());
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        CollectionFactory.toSet( chunk ),
                        blob.getLength(),
                        bucket )
                );
        dbSupport.getServiceManager().getService( JobEntryService.class ).delete( chunk.getId() );
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed to not retry.");
    }
    
    
   @Test
    public void testRunWithFailureToWriteFileResultsInRetry()
    {
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", -1 );
        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "key", "value" );
        mockDaoDriver.createObjectProperties( o2.getId(), propertiesMapping );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 2, 1000 );
        mockDaoDriver.updateBean( 
                blobs2.get( 0 ).setChecksum( "v2" ).setChecksumType( ChecksumType.values()[ 1 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.updateBean( 
                blobs2.get( 1 ).setChecksum( "v3" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries(
                        JobRequestType.PUT,
                        CollectionFactory.toSet( blob1, blobs2.get( 0 ), blobs2.get( 1 ) ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blobs2.get( 0 ).getId() );
        mockCacheFilesystemDriver.writeCacheFile( blobs2.get( 0 ).getId(), blobs2.get( 0 ).getLength() );
        cacheManager.blobLoadedToCache( blobs2.get( 0 ).getId() );
        
        cacheManager.allocateChunksForBlob( blobs2.get( 1 ).getId() );
        mockCacheFilesystemDriver.writeCacheFile( blobs2.get( 1 ).getId(), blobs2.get( 1 ).getLength() );
        cacheManager.blobLoadedToCache( blobs2.get( 1 ).getId() );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunks);
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                        .withDiskManager(cacheManager)
                        .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blobs2.get(0).getLength() + blobs2.get(1).getLength(),
                        bucket )
                );
        ds3Connection.setIsJobExistantResponse( true );
        ds3Connection.setGetBlobPersistenceResponse(
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs(
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ) );
        ds3Connection.setIsBucketExistantResponse( true );
        ds3Connection.setBlobsReady( new ArrayList<>() );
        ds3Connection.setPutBlobException( new RuntimeException( "Can't do that." ) );

        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Since chunk's not available, should notta proceeded to execute.");

        ds3Connection.setBlobsReady( CollectionFactory.toList( blob1.getId(), blobs2.get( 0 ).getId(), blobs2.get( 1 ).getId() ) );

        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended due to write error.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        ds3Connection.setPutBlobException( null );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated target failure.");

        final List< PutBlobRequest > objectPuts = ds3Connection.getPutBlobCalls();
        assertEquals(4,  objectPuts.size(), "Shoulda been a put for each blob, plus one more for the initial failure.");
        objectPuts.remove( 0 );
        final Map< String, Map< Long, Integer > > objectPutsMap = new HashMap<>();
        objectPutsMap.put( o1.getName(), new HashMap< Long, Integer >() );
        objectPutsMap.put( o2.getName(), new HashMap< Long, Integer >() );
        for ( final PutBlobRequest r : objectPuts )
        {
            objectPutsMap.get( r.getObjectName() ).put( 
                    Long.valueOf( r.getOffset() ),
                    Integer.valueOf( r.getMetadata().size() ) );
        }
        assertEquals(0,  objectPutsMap.get(o1.getName()).get(Long.valueOf(0)).intValue(), "Shoulda sent metadata on first blob only for each object.");
        assertEquals(1,  objectPutsMap.get(o2.getName()).get(Long.valueOf(0)).intValue(), "Shoulda sent metadata on first blob only for each object.");
        assertEquals(1,  objectPutsMap.get(o2.getName()).get(Long.valueOf(1000)).intValue(), "Shoulda sent metadata on every blob only for each object.");

        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        final BlobPersistence bp2 = BeanFactory.newBean( BlobPersistence.class );
        final BlobPersistence bp3 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( blob1.getId() );
        bp1.setChecksum( blob1.getChecksum() );
        bp1.setChecksumType( blob1.getChecksumType() );
        bp2.setId( blobs2.get( 0 ).getId() );
        bp2.setChecksum( blobs2.get( 0 ).getChecksum() );
        bp2.setChecksumType( blobs2.get( 0 ).getChecksumType() );
        bp3.setId( blobs2.get( 1 ).getId() );
        bp3.setChecksum( blobs2.get( 1 ).getChecksum() );
        bp3.setChecksumType( blobs2.get( 1 ).getChecksumType() );
        final BlobPersistenceContainer blobPersistence =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        blobPersistence.setBlobs( new BlobPersistence [] { bp1, bp2, bp3 } );
        
        ds3Connection.setGetBlobPersistenceResponse( blobPersistence );
        ds3Connection.setGetChunkPendingTargetCommitResponse( Boolean.FALSE );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed since data transmitted.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda recorded all blobs on target.");

        mockCacheFilesystemDriver.shutdown();
    }
    
    
   @Test
    public void testRunWithFailureToWriteFileResultsInRetryWhenTargetAlreadyHasSomeBlobs()
    {
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", -1 );
        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "key", "value" );
        mockDaoDriver.createObjectProperties( o2.getId(), propertiesMapping );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 2, 1000 );
        mockDaoDriver.updateBean( 
                blobs2.get( 0 ).setChecksum( "v2" ).setChecksumType( ChecksumType.values()[ 1 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.updateBean( 
                blobs2.get( 1 ).setChecksum( "v3" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries(
                        JobRequestType.PUT,
                        CollectionFactory.toSet( blob1, blobs2.get( 0 ), blobs2.get( 1 ) ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blobs2.get( 0 ).getId() );
        mockCacheFilesystemDriver.writeCacheFile( blobs2.get( 0 ).getId(), blobs2.get( 0 ).getLength() );
        cacheManager.blobLoadedToCache( blobs2.get( 0 ).getId() );
        
        cacheManager.allocateChunksForBlob( blobs2.get( 1 ).getId() );
        mockCacheFilesystemDriver.writeCacheFile( blobs2.get( 1 ).getId(), blobs2.get( 1 ).getLength() );
        cacheManager.blobLoadedToCache( blobs2.get( 1 ).getId() );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunks);
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                        .withDiskManager(cacheManager)
                        .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blobs2.get(0).getLength() + blobs2.get(1).getLength(),
                        bucket )
                );
        final BlobPersistence definedBp1 = BeanFactory.newBean( BlobPersistence.class );
        definedBp1.setId( blob1.getId() );
        final BlobPersistence definedBp2 = BeanFactory.newBean( BlobPersistence.class );
        definedBp2.setId( blobs2.get( 0 ).getId() );
        definedBp2.setChecksum( blobs2.get( 0 ).getChecksum() );
        definedBp2.setChecksumType( blobs2.get( 0 ).getChecksumType() );
        final BlobPersistenceContainer definedBpContainer = 
                BeanFactory.newBean( BlobPersistenceContainer.class );
        definedBpContainer.setBlobs( new BlobPersistence [] { definedBp1, definedBp2 } );
        
        ds3Connection.setIsJobExistantResponse( true );
        ds3Connection.setGetBlobPersistenceResponse( definedBpContainer );
        ds3Connection.setIsBucketExistantResponse( true );
        ds3Connection.setBlobsReady( new ArrayList<>() );
        ds3Connection.setPutBlobException( new RuntimeException( "Can't do that." ) );

        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Since chunk's not available, should notta proceeded to execute.");

        ds3Connection.setBlobsReady( CollectionFactory.toList( blob1.getId(), blobs2.get( 0 ).getId(), blobs2.get( 1 ).getId() ) );

        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended due to write error.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        ds3Connection.setPutBlobException( null );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated target failure.");

        final List< PutBlobRequest > objectPuts = ds3Connection.getPutBlobCalls();
        assertEquals(3,  objectPuts.size(), "Shoulda been a put for each blob except one, plus one more for the initial failure.");
        objectPuts.remove( 0 );
        final Map< String, Map< Long, Integer > > objectPutsMap = new HashMap<>();
        objectPutsMap.put( o1.getName(), new HashMap< Long, Integer >() );
        objectPutsMap.put( o2.getName(), new HashMap< Long, Integer >() );
        for ( final PutBlobRequest r : objectPuts )
        {
            objectPutsMap.get( r.getObjectName() ).put( 
                    Long.valueOf( r.getOffset() ),
                    Integer.valueOf( r.getMetadata().size() ) );
        }
        assertEquals(0,  objectPutsMap.get(o1.getName()).get(Long.valueOf(0)).intValue(), "Shoulda sent metadata on first blob only for each object.");
        assertNull(objectPutsMap.get( o2.getName() ).get( Long.valueOf( 0 ) ), "Should notta re-transmitted blob.");
        assertEquals(1,  objectPutsMap.get(o2.getName()).get(Long.valueOf(1000)).intValue(), "Shoulda sent metadata on every blob for each object.");

        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        final BlobPersistence bp2 = BeanFactory.newBean( BlobPersistence.class );
        final BlobPersistence bp3 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( blob1.getId() );
        bp1.setChecksum( blob1.getChecksum() );
        bp1.setChecksumType( blob1.getChecksumType() );
        bp2.setId( blobs2.get( 0 ).getId() );
        bp2.setChecksum( blobs2.get( 0 ).getChecksum() );
        bp2.setChecksumType( blobs2.get( 0 ).getChecksumType() );
        bp3.setId( blobs2.get( 1 ).getId() );
        bp3.setChecksum( blobs2.get( 1 ).getChecksum() );
        bp3.setChecksumType( blobs2.get( 1 ).getChecksumType() );
        final BlobPersistenceContainer blobPersistence =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        blobPersistence.setBlobs( new BlobPersistence [] { bp1, bp2, bp3 } );
        
        ds3Connection.setGetBlobPersistenceResponse( blobPersistence );
        ds3Connection.setGetChunkPendingTargetCommitResponse( Boolean.FALSE );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed since data transmitted.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda recorded all blobs on target.");

        mockCacheFilesystemDriver.shutdown();
    }
    
    
   @Test
    public void testRunAndReplicatePutJobToTargetFailsGeneratesFailure()
    {
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.updateBean( 
                blob2.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunks);
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                        .withDiskManager(cacheManager)
                        .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket )
                );
        ds3Connection.setIsJobExistantResponse( false );
        ds3Connection.setGetBlobPersistenceResponse(
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs( 
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ) );
        ds3Connection.setIsBucketExistantResponse( true );
        ds3Connection.setPutBlobException( null );
        
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        final Method mReplicatePutJob = ReflectUtil.getMethod( Ds3Connection.class, "replicatePutJob" );
        final Object expected = bucket.getName();
        assertEquals(expected, ds3Connection.getBtih().getMethodInvokeData( mReplicatePutJob ).get( 0 ).getArgs().get( 1 ), "Shoulda attempted replicated PUT job.");

        mockCacheFilesystemDriver.shutdown();
    }
    
    
   @Test
    public void testRunWontReplicateAnyDataIfEverythingAlreadyPersisted()
    {
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.updateBean( 
                blob2.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunks);
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                        .withDiskManager(cacheManager)
                        .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket )
                );

        final BlobPersistence definedBp1 = BeanFactory.newBean( BlobPersistence.class );
        definedBp1.setId( blob1.getId() );
        definedBp1.setChecksum( blob1.getChecksum() );
        definedBp1.setChecksumType( blob1.getChecksumType() );
        final BlobPersistence definedBp2 = BeanFactory.newBean( BlobPersistence.class );
        definedBp2.setId( blob2.getId() );
        definedBp2.setChecksum( blob2.getChecksum() );
        definedBp2.setChecksumType( blob2.getChecksumType() );
        final BlobPersistenceContainer definedBpContainer = 
                BeanFactory.newBean( BlobPersistenceContainer.class );
        definedBpContainer.setBlobs( new BlobPersistence [] { definedBp1, definedBp2 } );
        
        ds3Connection.setGetBlobPersistenceResponse( definedBpContainer );
        ds3Connection.setGetChunkPendingTargetCommitResponse( null );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed since no remaining targets to replicate to.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated target failure.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda recorded all blobs on target.");

        mockCacheFilesystemDriver.shutdown();
    }
    
    
   @Test
    public void testRunAndChecksumMismatchOccursUponCommitVerifyGeneratesFailure()
    {
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.updateBean( 
                blob2.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunks);
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                        .withDiskManager(cacheManager)
                        .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket )
                );
        ds3Connection.setGetBlobPersistenceResponse(
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs(
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) )
                        .setJobExistant( true ) );
        ds3Connection.setIsBucketExistantResponse( true );
        ds3Connection.setBlobsReady( CollectionFactory.toList( blob1.getId(), blob2.getId() ) );
        ds3Connection.setPutBlobException( null );
        ds3Connection.setReplicatePutJobException( null );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        ds3Connection.setGetChunkPendingTargetCommitResponse( null );
        
        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        final BlobPersistence bp2 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( blob1.getId() );
        bp1.setChecksum( blob1.getChecksum() );
        bp1.setChecksumType( blob1.getChecksumType() );
        bp2.setId( blob2.getId() );
        bp2.setChecksum( "wrong!" );
        bp2.setChecksumType( blob2.getChecksumType() );
        final BlobPersistenceContainer blobPersistence =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        blobPersistence.setBlobs( new BlobPersistence [] { bp1, bp2 } );
        ds3Connection.setGetBlobPersistenceResponse( blobPersistence );

        task.prepareForExecutionIfPossible();

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready since remaining target to replicate to.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        mockCacheFilesystemDriver.shutdown();
    }
    
    
   @Test
    public void testRunWillReplicatePutJobToTargetIfDoesNotAlreadyExist()
    {
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.updateBean( 
                blob2.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunks);
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                        .withDiskManager(cacheManager)
                        .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket )
                );
        ds3Connection.setIsJobExistantResponse( false );
        ds3Connection.setGetBlobPersistenceResponse(
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs(
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ) );
        ds3Connection.setIsBucketExistantResponse( true );
        ds3Connection.setBlobsReady( CollectionFactory.toList( blob1.getId(), blob2.getId() ) );
        ds3Connection.setPutBlobException( null );
        ds3Connection.setReplicatePutJobException( null );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        ds3Connection.setGetChunkPendingTargetCommitResponse( null );
        
        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        final BlobPersistence bp2 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( blob1.getId() );
        bp1.setChecksum( blob1.getChecksum() );
        bp1.setChecksumType( blob1.getChecksumType() );
        bp2.setId( blob2.getId() );
        bp2.setChecksum( blob2.getChecksum() );
        bp2.setChecksumType( blob2.getChecksumType() );
        final BlobPersistenceContainer blobPersistence =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        blobPersistence.setBlobs( new BlobPersistence [] { bp1, bp2 } );
        ds3Connection.setGetBlobPersistenceResponse( blobPersistence );

        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed since no remaining targets to replicate to.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated target failure.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda recorded all blobs on target.");

        final Method mReplicatePutJob = ReflectUtil.getMethod( Ds3Connection.class, "replicatePutJob" );
        final Object expected = bucket.getName();
        assertEquals(expected, ds3Connection.getBtih().getMethodInvokeData( mReplicatePutJob ).get( 0 ).getArgs().get( 1 ), "Shoulda attempted replicated PUT job.");

        mockCacheFilesystemDriver.shutdown();
    }
    
    
   @Test
    public void testRunWillReplicatePutJobToTargetAndCreateBucketIfNeitherAlreadyExist()
    {
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.updateBean( 
                blob2.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        
        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunks);
                final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                        .withDiskManager(cacheManager)
                        .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket )
                );
        ds3Connection.setIsJobExistantResponse( false );
        ds3Connection.setGetBlobPersistenceResponse(
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs(
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ) );
        ds3Connection.setIsBucketExistantResponse( false );
        ds3Connection.setBlobsReady( CollectionFactory.toList( blob1.getId(), blob2.getId() ) );
        ds3Connection.setPutBlobException( null );
        ds3Connection.setReplicatePutJobException( null );
        ds3Connection.setCreateBucketException( null );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        ds3Connection.setGetChunkPendingTargetCommitResponse( null );
        
        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        final BlobPersistence bp2 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( blob1.getId() );
        bp1.setChecksum( blob1.getChecksum() );
        bp1.setChecksumType( blob1.getChecksumType() );
        bp2.setId( blob2.getId() );
        bp2.setChecksum( blob2.getChecksum() );
        bp2.setChecksumType( blob2.getChecksumType() );
        final BlobPersistenceContainer blobPersistence =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        blobPersistence.setBlobs( new BlobPersistence [] { bp1, bp2 } );
        ds3Connection.setGetBlobPersistenceResponse( blobPersistence );

        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed since no remaining targets to replicate to.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated target failure.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda recorded all blobs on target.");

        final Method mReplicatePutJob = ReflectUtil.getMethod( Ds3Connection.class, "replicatePutJob" );
        final Object expected = bucket.getName();
        assertEquals(expected, ds3Connection.getBtih().getMethodInvokeData( mReplicatePutJob ).get( 0 ).getArgs().get( 1 ), "Shoulda attempted replicated PUT job.");

        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testRunWithSuspectBlobs()
    {
        dbSupport.reset();
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();

        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean(
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ),
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", -1 );
        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "key", "value" );
        mockDaoDriver.createObjectProperties( o2.getId(), propertiesMapping );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 2, 1000 );
        mockDaoDriver.updateBean(
                blobs2.get( 0 ).setChecksum( "v2" ).setChecksumType( ChecksumType.values()[ 1 ] ),
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.updateBean(
                blobs2.get( 1 ).setChecksum( "v3" ).setChecksumType( ChecksumType.values()[ 0 ] ),
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries(
                        JobRequestType.PUT,
                        CollectionFactory.toSet( blob1, blobs2.get( 0 ), blobs2.get( 1 ) ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );

        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );

        cacheManager.allocateChunksForBlob( blobs2.get( 0 ).getId() );
        mockCacheFilesystemDriver.writeCacheFile( blobs2.get( 0 ).getId(), blobs2.get( 0 ).getLength() );
        cacheManager.blobLoadedToCache( blobs2.get( 0 ).getId() );

        cacheManager.allocateChunksForBlob( blobs2.get( 1 ).getId() );
        mockCacheFilesystemDriver.writeCacheFile( blobs2.get( 1 ).getId(), blobs2.get( 1 ).getLength() );
        cacheManager.blobLoadedToCache( blobs2.get( 1 ).getId() );

        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunks);
        final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blobs2.get(0).getLength() + blobs2.get(1).getLength(),
                        bucket )
                );
        final BlobPersistence definedBp1 = BeanFactory.newBean( BlobPersistence.class );
        definedBp1.setId( blob1.getId() );
        definedBp1.setChecksum( blob1.getChecksum() );
        definedBp1.setChecksumType( blob1.getChecksumType() );
        final BlobPersistence definedBp2 = BeanFactory.newBean( BlobPersistence.class );
        definedBp2.setId( blobs2.get( 0 ).getId() );
        definedBp2.setChecksum( blobs2.get( 0 ).getChecksum() );
        definedBp2.setChecksumType( blobs2.get( 0 ).getChecksumType() );
        final BlobPersistenceContainer definedBpContainer =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        definedBpContainer.setBlobs( new BlobPersistence [] { definedBp1, definedBp2 } );

        ds3Connection.setIsJobExistantResponse( true );
        ds3Connection.setGetBlobPersistenceResponse( definedBpContainer );
        ds3Connection.setIsBucketExistantResponse( true );
        ds3Connection.setBlobsReady( new ArrayList<>() );

        BlobDs3Target markTarget = BeanFactory.newBean(BlobDs3Target.class);
        markTarget.setBlobId(blob1.getId());
        markTarget.setTargetId(target.getId());
        mockDaoDriver.makeSuspect(
                mockDaoDriver.putBlobOnDs3Target( target.getId(), blob1.getId() ) );



        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Since chunk's not available, should notta proceeded to execute.");

        ds3Connection.setBlobsReady( CollectionFactory.toList( blob1.getId(), blobs2.get( 0 ).getId(), blobs2.get( 1 ).getId() ) );

        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended due to write error.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        ds3Connection.setPutBlobException( null );

        task.prepareForExecutionIfPossible();

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated target failure.");



        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported task NOT_READY, since one blob was marked suspect.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobDs3Target.class).getCount());

        mockCacheFilesystemDriver.shutdown();
    }


   @Test
    public void testPoolReadFailureDuringIomMigrationMarksDataMigrationInError()
    {
        final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();

        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean(
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ),
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean(
                dbSupport.getServiceManager().getRetriever(Job.class).attain(job.getId())
                        .setIomType(IomType.STANDARD_IOM),
                Job.IOM_TYPE);
        final JobEntry chunk = mockDaoDriver.createJobEntry(job.getId(), blob1);

        final DataMigration migration = BeanFactory.newBean(DataMigration.class)
                .setPutJobId(job.getId());
        dbSupport.getServiceManager().getCreator(DataMigration.class).create(migration);

        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );

        final BlobPool blobPool = mockDaoDriver.putBlobOnPool(
                mockDaoDriver.createPool(mockDaoDriver.createPoolPartition(null, "dp1").getId(), null).getId(),
                blob1.getId());

        final DiskManager diskManager = InterfaceProxyFactory.getProxy( DiskManager.class,
                ( proxy, method, args ) -> {
                    if ( "getDiskFileFor".equals( method.getName() ) ) {
                        return BeanFactory.newBean( DiskFileInfo.class )
                                .setBlobPoolId( blobPool.getId() );
                        // filePath intentionally null to trigger NullPointerException in new File(null)
                    }
                    return null;
                } );

        final Set<Ds3BlobDestination> pts = mockDaoDriver.createReplicationTargetsForChunks(
                Ds3DataReplicationRule.class, Ds3BlobDestination.class, Set.of(chunk));
        final WriteChunkToDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(diskManager)
                .withDs3ConnectionFactory(ds3Connection).buildWriteChunkToDs3TargetTask(new TargetWriteDirective<>(
                        Ds3Target.class,
                        pts,
                        target,
                        BlobStoreTaskPriority.values()[ 0 ],
                        Set.of(chunk),
                        blob1.getLength(),
                        bucket));

        ds3Connection.setIsJobExistantResponse( true );
        ds3Connection.setGetBlobPersistenceResponse(
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs(
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ) );
        ds3Connection.setIsBucketExistantResponse( true );
        ds3Connection.setBlobsReady( CollectionFactory.toList( blob1.getId() ) );

        assertFalse(dbSupport.getServiceManager().getRetriever(DataMigration.class)
                        .attain(migration.getId()).isInError(),
                "Migration should not be in error yet.");

        for (int i = 0; i < 3; i++) {
            task.prepareForExecutionIfPossible();
            TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {
                    TestUtil.invokeAndWaitChecked( task );
                }
            } );
        }

        assertTrue(dbSupport.getServiceManager().getRetriever(DataMigration.class)
                        .attain(migration.getId()).isInError(),
                "Data migration should be marked in error after pool read failures.");

        mockCacheFilesystemDriver.shutdown();
    }


    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    @BeforeAll
     static void resetDb() { dbSupport.reset(); }

    @AfterEach
     void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        dbSupport.reset();
    }

}
