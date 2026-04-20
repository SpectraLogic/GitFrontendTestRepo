/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.S3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.s3.dataplanner.testfrmwrk.TargetTaskBuilder;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public final class WriteChunkToS3TargetTask_Test
{
    @Test
    public void testHappyConstruction()
    {
        new WriteChunkToS3TargetTask(
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                InterfaceProxyFactory.getProxy( DiskManager.class, null ),
                new MockBeansServiceManager(),
                new MockS3ConnectionFactory(),
                new TargetWriteDirective<>(S3Target.class,
                        new HashSet<S3BlobDestination>(),
                        BeanFactory.newBean(S3Target.class),
                        BlobStoreTaskPriority.NORMAL,
                        new HashSet<>(),
                        0,
                        null));
    }
    

    @Test
    public void testPrepareForExecutionSelectsNoS3TargetIfSelectionNotPossibleDueToState()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.updateBean( target.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );

        final Set<S3BlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(S3DataReplicationRule.class, S3BlobDestination.class, chunks);
        final WriteChunkToS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildWriteChunkToS3TargetTask(
                new TargetWriteDirective<>(S3Target.class,
                        chunkTargets,
                        target,
                        BlobStoreTaskPriority.NORMAL,
                        chunks,
                        blob.getLength(),
                        bucket));

        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready to run to retry.");
    }


    @Test
    public void testPrepareForExecutionSelectsNoS3TargetIfSelectionNotPossibleDueToQuiesced()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );

        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.updateBean( target.setQuiesced( Quiesced.PENDING ), ReplicationTarget.QUIESCED );

        final Set<S3BlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(S3DataReplicationRule.class, S3BlobDestination.class, chunks);
        final WriteChunkToS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildWriteChunkToS3TargetTask(
                new TargetWriteDirective<>(S3Target.class,
                        chunkTargets,
                        target,
                        BlobStoreTaskPriority.NORMAL,
                        chunks,
                        blob.getLength(),
                        bucket));

        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready to run to retry.");
    }


    @Test
    public void testPrepareForExecutionSelectsNoS3TargetIfSelectionNotPossibleDueToFailureToConnect()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );

        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.updateBean( target.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final Set<S3BlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(S3DataReplicationRule.class, S3BlobDestination.class, chunks);
        final WriteChunkToS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildWriteChunkToS3TargetTask(
                new TargetWriteDirective<>(S3Target.class,
                        chunkTargets,
                        target,
                        BlobStoreTaskPriority.NORMAL,
                        chunks,
                        blob.getLength(),
                        bucket));

        final MockS3ConnectionFactory s3ConnectionFactory = new MockS3ConnectionFactory();
        s3ConnectionFactory.setConnectException( new RuntimeException( "Can't connect." ) );

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
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final Set<S3BlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunk(S3DataReplicationRule.class, S3BlobDestination.class, chunk.getId());
        final WriteChunkToS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildWriteChunkToS3TargetTask(
                new TargetWriteDirective<>(S3Target.class,
                        chunkTargets,
                        target,
                        BlobStoreTaskPriority.NORMAL,
                        Set.of(chunk),
                        blob.getLength(),
                        bucket));

        dbSupport.getServiceManager().getService( JobEntryService.class ).delete( chunk.getId() );
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed to not retry.");
    }


    @Test
    public void testRunWhenCannotConnectResultsInFailure()
    {
        final MockS3ConnectionFactory s3Connection = new MockS3ConnectionFactory();
        
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
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.createS3DataReplicationRule(
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

        final Set<S3BlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(S3DataReplicationRule.class, S3BlobDestination.class, chunks);
        final WriteChunkToS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3Connection).buildWriteChunkToS3TargetTask(
                new TargetWriteDirective<>(S3Target.class,
                        chunkTargets,
                        target,
                        BlobStoreTaskPriority.NORMAL,
                        chunks,
                        blobs2.stream().mapToLong(Blob::getLength).sum(),
                        bucket));

        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended due to connection error.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        mockCacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithFailureToWriteFileResultsInRetry()
    {
        final MockS3ConnectionFactory s3Connection = new MockS3ConnectionFactory();
        
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
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.createS3DataReplicationRule(
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

        final Set<S3BlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(S3DataReplicationRule.class, S3BlobDestination.class, chunks);
        final WriteChunkToS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3Connection).buildWriteChunkToS3TargetTask(
                new TargetWriteDirective<>(S3Target.class,
                        chunkTargets,
                        target,
                        BlobStoreTaskPriority.NORMAL,
                        chunks,
                        blobs2.stream().mapToLong(Blob::getLength).sum(),
                        bucket));

        final Method methodWrite = ReflectUtil.getMethod( PublicCloudConnection.class, "writeBlobToCloud" );
        s3Connection.setIh( MockInvocationHandler.forMethod(
                methodWrite,
                new InvocationHandler()
                {
                    public Object invoke(
                            final Object proxy,
                            final Method method,
                            final Object[] args ) throws Throwable
                    {
                        throw new RuntimeException( "Can't write to cloud." );
                    }
                },
                null ) );

        s3Connection.setConnectException( null );
        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended due to write error.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        s3Connection.setIh( null );

        task.prepareForExecutionIfPossible();

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Should notta generated target failure.");

        final List< MethodInvokeData > objectPuts =
                s3Connection.getBtihs().get( 1 ).getMethodInvokeData( methodWrite );
        assertEquals(3,  objectPuts.size(), "Shoulda been a put for each blob.");
        mockCacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWhenIncompatibleCloudBucketNotAllowed()
    {
        final MockS3ConnectionFactory s3Connection = new MockS3ConnectionFactory();
        
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
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.createS3DataReplicationRule(
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

        final Set<S3BlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(S3DataReplicationRule.class, S3BlobDestination.class, chunks);
        final WriteChunkToS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3Connection).buildWriteChunkToS3TargetTask(
                new TargetWriteDirective<>(S3Target.class,
                        chunkTargets,
                        target,
                        BlobStoreTaskPriority.NORMAL,
                        chunks,
                        blobs2.stream().mapToLong(Blob::getLength).sum(),
                        bucket));

        final Method methodGetBucket =
                ReflectUtil.getMethod( PublicCloudConnection.class, "getExistingBucketInformation" );
        final Method methodCreateOrTakeoverBucket =
                ReflectUtil.getMethod( PublicCloudConnection.class, "createOrTakeoverBucket" );
        
        s3Connection.setConnectException(null);
        
        s3Connection.setIh( MockInvocationHandler.forMethod(
                methodGetBucket,
                new ConstantResponseInvocationHandler(
                        BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( UUID.randomUUID() ) ),
                null ) );
        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "owned by another appliance" ), "Failure message shoulda been helpful.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        s3Connection.setIh( MockInvocationHandler.forMethod(
                methodGetBucket,
                new ConstantResponseInvocationHandler(
                        BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly( 
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 33 ) ),
                null ) );
        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        TestUtil.invokeAndWaitChecked( task );
                    }
                } ).getMessage().contains( "this version of software only supports" ), "Failure message shoulda been helpful.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobS3Target.class).getCount(), "Should notta recorded blobs on target.");

        s3Connection.setIh( MockInvocationHandler.forMethod(
                methodGetBucket,
                new ConstantResponseInvocationHandler(
                        BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( "somethingelse" )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() ) ),
                null ) );
        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "cloud bucket to be for" ), "Failure message shoulda been helpful.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobS3Target.class).getCount(), "Should notta recorded blobs on target.");

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                methodGetBucket,
                new ConstantResponseInvocationHandler(
                        BeanFactory.newBean( PublicCloudBucketInformation.class ) ),
                null ) );
        s3Connection.setIh( btih );
        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(1,  btih.getMethodCallCount(methodCreateOrTakeoverBucket), "Shoulda taken over existing bucket.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobS3Target.class).getCount(), "Shoulda recorded blobs on target.");

        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testPoolReadFailureDuringIomMigrationMarksDataMigrationInError()
    {
        final MockS3ConnectionFactory s3Connection = new MockS3ConnectionFactory();

        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean(
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ),
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.createS3DataReplicationRule(
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
                    }
                    return null;
                } );

        final Set<S3BlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(
                S3DataReplicationRule.class, S3BlobDestination.class, Set.of(chunk));
        final WriteChunkToS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(diskManager)
                .withS3ConnectionFactory(s3Connection).buildWriteChunkToS3TargetTask(
                new TargetWriteDirective<>(S3Target.class,
                        chunkTargets,
                        target,
                        BlobStoreTaskPriority.NORMAL,
                        Set.of(chunk),
                        blob1.getLength(),
                        bucket));

        s3Connection.setConnectException( null );

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
    public static void resetDb() { dbSupport.reset(); }


    @AfterEach
    public void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        dbSupport.reset();
    }

}
