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

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
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
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public final class WriteChunkToAzureTargetTask_Test
{
    @Test
    public void testHappyConstruction()
    {
        new WriteChunkToAzureTargetTask(
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                InterfaceProxyFactory.getProxy(DiskManager.class, null),
                InterfaceProxyFactory.getProxy(BeansServiceManager.class, null),
                InterfaceProxyFactory.getProxy(AzureConnectionFactory.class, null),
                new TargetWriteDirective<>(
                        AzureTarget.class,
                        Set.<AzureBlobDestination>of(),
                        BeanFactory.newBean(AzureTarget.class),
                        BlobStoreTaskPriority.values()[ 0 ],
                        Set.of(),
                        0L,
                        BeanFactory.newBean(Bucket.class)));
    }

    @Test
    public void testPrepareForExecutionSelectsNoAzureTargetIfSelectionNotPossibleDueToState()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.updateBean( target.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );

        final DiskManager diskManager = new MockDiskManager(dbSupport.getServiceManager());
        final AzureConnectionFactory connectionFactory = new MockAzureConnectionFactory();
        final WriteChunkToAzureTargetTask task = new TargetTaskBuilder(mockDaoDriver.getServiceManager()).withDiskManager(diskManager)
                .withAzureConnectionFactory(connectionFactory)
                .buildWriteChunkToAzureTargetTask(
                        getWriteDirectiveFor(Set.of(chunk), mockDaoDriver));

        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready to run to retry.");
    }
    

    @Test
    public void testPrepareForExecutionSelectsNoAzureTargetIfSelectionNotPossibleDueToQuiesced()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.updateBean( target.setQuiesced( Quiesced.PENDING ), ReplicationTarget.QUIESCED );

        final DiskManager diskManager = new MockDiskManager(dbSupport.getServiceManager());
        final AzureConnectionFactory connectionFactory = new MockAzureConnectionFactory();
        final WriteChunkToAzureTargetTask task = new TargetTaskBuilder(mockDaoDriver.getServiceManager()).withDiskManager(diskManager)
                .withAzureConnectionFactory(connectionFactory)
                .buildWriteChunkToAzureTargetTask(
                        getWriteDirectiveFor(Set.of(chunk), mockDaoDriver));

        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready to run to retry.");
    }
    

    @Test
    public void testPrepareForExecutionSelectsNoAzureTargetIfSelectionNotPossibleDueToFailureToConnect()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.updateBean( target.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
        mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final MockAzureConnectionFactory azureConnectionFactory = new MockAzureConnectionFactory();
        final DiskManager diskManager = new MockDiskManager(dbSupport.getServiceManager());
        final WriteChunkToAzureTargetTask task = new TargetTaskBuilder(mockDaoDriver.getServiceManager()).withDiskManager(diskManager)
                .withAzureConnectionFactory(azureConnectionFactory)
                .buildWriteChunkToAzureTargetTask(
                        getWriteDirectiveFor(Set.of(chunk), mockDaoDriver));

        azureConnectionFactory.setConnectException( new RuntimeException( "Can't connect." ) );

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
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final DiskManager diskManager = new MockDiskManager(dbSupport.getServiceManager());
        final AzureConnectionFactory connectionFactory = new MockAzureConnectionFactory();
        final WriteChunkToAzureTargetTask task = new TargetTaskBuilder(mockDaoDriver.getServiceManager()).withDiskManager(diskManager)
                .withAzureConnectionFactory(connectionFactory)
                .buildWriteChunkToAzureTargetTask(
                        getWriteDirectiveFor(Set.of(chunk), mockDaoDriver));

        dbSupport.getServiceManager().getService( JobEntryService.class ).delete( chunk.getId() );
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed to not retry.");
    }
    
    
    @Test
    public void testRunWhenCannotConnectResultsInFailure()
    {
        final MockAzureConnectionFactory azureConnection = new MockAzureConnectionFactory();
        
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
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.createAzureDataReplicationRule(
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

        final WriteChunkToAzureTargetTask task = new TargetTaskBuilder(mockDaoDriver.getServiceManager()).withDiskManager(cacheManager)
                .withAzureConnectionFactory(azureConnection)
                .buildWriteChunkToAzureTargetTask(
                        getWriteDirectiveFor(chunks, mockDaoDriver));

        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended due to connection error.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(AzureTargetFailure.class).getCount(), "Shoulda generated target failure.");

        mockCacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWithFailureToWriteFileResultsInRetry()
    {
        final MockAzureConnectionFactory azureConnection = new MockAzureConnectionFactory();
        
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
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.createAzureDataReplicationRule(
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

        final WriteChunkToAzureTargetTask task = new TargetTaskBuilder(mockDaoDriver.getServiceManager()).withDiskManager(cacheManager)
                .withAzureConnectionFactory(azureConnection)
                .buildWriteChunkToAzureTargetTask(
                        getWriteDirectiveFor(chunks, mockDaoDriver));

        final Method methodWrite = ReflectUtil.getMethod( PublicCloudConnection.class, "writeBlobToCloud" );
        azureConnection.setIh( MockInvocationHandler.forMethod(
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
        
        azureConnection.setConnectException( null );
        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended due to write error.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(AzureTargetFailure.class).getCount(), "Shoulda generated target failure.");

        azureConnection.setIh( null );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(1,  dbSupport.getServiceManager().getRetriever(AzureTargetFailure.class).getCount(), "Should notta generated target failure.");

        final List< MethodInvokeData > objectPuts = 
                azureConnection.getBtihs().get( 1 ).getMethodInvokeData( methodWrite );
        assertEquals(3,  objectPuts.size(), "Shoulda been a put for each blob.");
        mockCacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWhenIncompatibleCloudBucketNotAllowed()
    {
        final MockAzureConnectionFactory azureConnection = new MockAzureConnectionFactory();
        
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
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.createAzureDataReplicationRule(
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

        final WriteChunkToAzureTargetTask task = new TargetTaskBuilder(mockDaoDriver.getServiceManager()).withDiskManager(cacheManager)
                .withAzureConnectionFactory(azureConnection)
                .buildWriteChunkToAzureTargetTask(
                        getWriteDirectiveFor(chunks, mockDaoDriver));

        final Method methodGetBucket =
                ReflectUtil.getMethod( PublicCloudConnection.class, "getExistingBucketInformation" );
        azureConnection.setIh( MockInvocationHandler.forMethod(
                methodGetBucket,
                new ConstantResponseInvocationHandler(
                        BeanFactory.newBean( PublicCloudBucketInformation.class ) ),
                null ) );
        
        azureConnection.setConnectException( null );
        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        TestUtil.invokeAndWaitChecked( task );
                    }
                } ).getMessage().contains( "not formatted for use by this appliance" ), "Failure message shoulda been helpful.");
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended due to write error.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(AzureTargetFailure.class).getCount(), "Shoulda generated target failure.");

        azureConnection.setIh( MockInvocationHandler.forMethod(
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
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(AzureTargetFailure.class).getCount(), "Shoulda generated target failure.");

        azureConnection.setIh( MockInvocationHandler.forMethod(
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
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(AzureTargetFailure.class).getCount(), "Shoulda generated target failure.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobAzureTarget.class).getCount(), "Should notta recorded blobs on target.");

        azureConnection.setIh( MockInvocationHandler.forMethod(
                methodGetBucket,
                new ConstantResponseInvocationHandler(
                        BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly( 
                                DataPathBackend.class ).getInstanceId() )
                        .setLocalBucketName( "invalid" ) ),
                null ) );
        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        TestUtil.invokeAndWaitChecked( task );
                    }
                } ).getMessage().contains( "the cloud bucket to be for" ), "Failure message shoulda been helpful.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(AzureTargetFailure.class).getCount(), "Shoulda generated target failure.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobAzureTarget.class).getCount(), "Should notta recorded blobs on target.");

        azureConnection.setIh( MockInvocationHandler.forMethod(
                methodGetBucket,
                new ConstantResponseInvocationHandler(
                        BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly( 
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) ),
                null ) );
        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobAzureTarget.class).getCount(), "Shoulda recorded blobs on target.");

        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testPoolReadFailureDuringIomMigrationMarksDataMigrationInError()
    {
        final MockAzureConnectionFactory azureConnection = new MockAzureConnectionFactory();

        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean(
                blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ),
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.createAzureDataReplicationRule(
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

        final Set<AzureBlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(
                AzureDataReplicationRule.class, AzureBlobDestination.class, Set.of(chunk));
        final WriteChunkToAzureTargetTask task = new TargetTaskBuilder(mockDaoDriver.getServiceManager())
                .withDiskManager(diskManager)
                .withAzureConnectionFactory(azureConnection)
                .buildWriteChunkToAzureTargetTask(
                        new TargetWriteDirective<>(
                                AzureTarget.class,
                                chunkTargets,
                                target,
                                BlobStoreTaskPriority.values()[ 0 ],
                                Set.of(chunk),
                                blob1.getLength(),
                                bucket));

        azureConnection.setConnectException( null );

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

    private TargetWriteDirective getWriteDirectiveFor(Collection<JobEntry> entries, final MockDaoDriver mockDaoDriver) {
        final Set<AzureBlobDestination> chunkTargets = mockDaoDriver.createReplicationTargetsForChunks(AzureDataReplicationRule.class, AzureBlobDestination.class, entries);
        return new TargetWriteDirective<>(
                AzureTarget.class,
                chunkTargets,
                mockDaoDriver.attainOneAndOnly(AzureTarget.class),
                BlobStoreTaskPriority.values()[ 0 ],
                entries,
                mockDaoDriver.retrieveAll(Blob.class).stream().mapToLong(Blob::getLength).sum(),
                mockDaoDriver.attainOneAndOnly(Bucket.class));
    }

    private static DatabaseSupport dbSupport ;
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }

}
