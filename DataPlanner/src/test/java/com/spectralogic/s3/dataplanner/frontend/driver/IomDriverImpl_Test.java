/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.driver;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.*;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.tape.SuspectBlobTapeService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobAzureTargetService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobS3TargetService;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.target.task.MockDs3ConnectionFactory;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


import java.lang.reflect.Field;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class IomDriverImpl_Test {

    @Test
    public void testAllBlobsInCache() {
        cacheManager.blobLoadedToCache( b1.getId() );
        cacheManager.blobLoadedToCache( b2.getId() );
        cacheManager.blobLoadedToCache( b3.getId() );

        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                new MockDs3ConnectionFactory(),
                1000);

        test.removeUnavailableBlobs(testBlobs);
        
        assertEquals(3, testBlobs.size(), "All blobs are in cache");
    }

    @Test
    public void testAllBlobsInCacheSuspects() {
        S3Target target = mockDaoDriver.createS3Target("s3Target");
        BlobS3Target b1Target =  mockDaoDriver.putBlobOnS3Target(target.getId(), b1.getId());

        //Add to suspect table
        final SuspectBlobS3Target bean = BeanFactory.newBean( SuspectBlobS3Target.class );
        BeanCopier.copy( bean, b1Target );
        dbSupport.getServiceManager().getService( SuspectBlobS3TargetService.class ).create( bean );

        cacheManager.blobLoadedToCache( b1.getId() );


        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                new MockDs3ConnectionFactory(),
                1000);

        test.removeUnavailableBlobs(testBlobs);

        assertEquals(1, testBlobs.size(), "All blobs are in cache");
    }

    @Test
    public void testNoBlobsInCacheOtherTargets() {

        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                new MockDs3ConnectionFactory(),
                1000);

        test.removeUnavailableBlobs(testBlobs);
        assertEquals(0, testBlobs.size(), "All blobs should be removed as none are available");
    }

    @Test
    public void testSomeBlobsOnOnlinePool() {
        mockDaoDriver.putBlobOnPool( enterprisePool.getId(), b1.getId());

        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                new MockDs3ConnectionFactory(),
                1000);

        test.removeUnavailableBlobs(testBlobs);

        assertEquals(1, testBlobs.size(), "One blob is available in online pool");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should be available");
    }

    @Test
    public void testSomeBlobsOnNearlinePool() {
        mockDaoDriver.putBlobOnPool( archivePool.getId(), b1.getId());

        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                new MockDs3ConnectionFactory(),
                1000);

        test.removeUnavailableBlobs(testBlobs);

        assertEquals(1, testBlobs.size(), "One blob is available in online pool");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should be available");
    }

    @Test
    public void testSomeBlobsOnTape() {
        mockDaoDriver.putBlobsOnTape( t1.getId(), b1);

        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                new MockDs3ConnectionFactory(),
                1000);

        test.removeUnavailableBlobs(testBlobs);

        assertEquals(0, testBlobs.size(), "One blob is available in online pool");
        assertFalse(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should not be available");
    }

    @Test
    public void testBlobsOnDs3Target() {
        final BlobPersistenceContainer blobPersistenceContainer =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        List<BlobPersistence> blobPersistenceList = new ArrayList<>();
        for (Blob blob: testBlobs) {
            BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
            bp1.setId( blob.getId() );
            bp1.setAvailableOnTapeNow(true);
            blobPersistenceList.add( bp1 );
        }
        BlobPersistence[] blobPersistenceArray = blobPersistenceList.toArray(new BlobPersistence[0]);
        blobPersistenceContainer.setBlobs(blobPersistenceArray);

        Ds3Target target = mockDaoDriver.createDs3Target("ds3Target");
        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        mockConnection.setGetBlobPersistenceResponse(blobPersistenceContainer);
        mockDaoDriver.putBlobOnDs3Target(target.getId(), b1.getId());
        mockDaoDriver.putBlobOnDs3Target(target.getId(), b2.getId());
        mockDaoDriver.putBlobOnDs3Target(target.getId(), b3.getId());
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);

        test.removeUnavailableBlobs(testBlobs);

        assertEquals(3, testBlobs.size(), "One blob is available in online pool");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should be available");
    }

    @Test
    public void testBlobsOnDs3TargetQuiesced() {
        final BlobPersistenceContainer blobPersistenceContainer =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        List<BlobPersistence> blobPersistenceList = new ArrayList<>();
        for (Blob blob: testBlobs) {
            BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
            bp1.setId( blob.getId() );
            bp1.setAvailableOnTapeNow(true);
            blobPersistenceList.add( bp1 );
        }
        BlobPersistence[] blobPersistenceArray = blobPersistenceList.toArray(new BlobPersistence[0]);
        blobPersistenceContainer.setBlobs(blobPersistenceArray);

        Ds3Target target = mockDaoDriver.createDs3Target("ds3Target");
        mockDaoDriver.updateBean( target.setQuiesced(Quiesced.YES), Ds3Target.QUIESCED );
        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        mockConnection.setGetBlobPersistenceResponse(blobPersistenceContainer);
        mockDaoDriver.putBlobOnDs3Target(target.getId(), b1.getId());
        mockDaoDriver.putBlobOnDs3Target(target.getId(), b2.getId());
        mockDaoDriver.putBlobOnDs3Target(target.getId(), b3.getId());
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);

        test.removeUnavailableBlobs(testBlobs);

        assertEquals(0, testBlobs.size(), "One blob is available in online pool");
        assertFalse(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should be available");
    }

    @Test
    public void testBlobsOnS3Target() {
        S3Target target = mockDaoDriver.createS3Target("s3Target");
        mockDaoDriver.putBlobOnS3Target(target.getId(), b1.getId());
        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);
        test.removeUnavailableBlobs(testBlobs);
        assertEquals(1, testBlobs.size(), "One blob available on S3 target");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should be available");
    }

    @Test
    public void testBlobsOnAzureTarget() {
        AzureTarget target = mockDaoDriver.createAzureTarget("azureTarget");
        mockDaoDriver.putBlobOnAzureTarget(target.getId(), b1.getId());
        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);
        test.removeUnavailableBlobs(testBlobs);
        assertEquals(1, testBlobs.size(), "One blob available on S3 target");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should be available");

    }

    // Test with single copy which is a suspect - S3
    @Test
    public void testWithSuspectSingleCopyS3Blobs() {
        S3Target target = mockDaoDriver.createS3Target("s3Target");
        BlobS3Target b1Target =  mockDaoDriver.putBlobOnS3Target(target.getId(), b1.getId());
        mockDaoDriver.putBlobOnS3Target(target.getId(), b2.getId());
        mockDaoDriver.putBlobOnS3Target(target.getId(), b3.getId());
        //Add to suspect table
        final SuspectBlobS3Target bean = BeanFactory.newBean( SuspectBlobS3Target.class );
        BeanCopier.copy( bean, b1Target );
        dbSupport.getServiceManager().getService( SuspectBlobS3TargetService.class ).create( bean );

        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);
        test.removeUnavailableBlobs(testBlobs);
        assertEquals(3, testBlobs.size(), "One blob available on S3 target");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b2.getId())), "blob2 should be available");

    }

    // Test with single copy which is a suspect - S3
    @Test
    public void testWithSuspectSingleCopyTapeBlobs() {
        S3Target target = mockDaoDriver.createS3Target("s3Target");
        List<BlobTape>  blobsOnTape =  mockDaoDriver.putBlobsOnTape(t1.getId(), b1);
        mockDaoDriver.putBlobsOnTape(t2.getId(), b2);
        mockDaoDriver.putBlobOnS3Target(target.getId(), b2.getId());
        mockDaoDriver.putBlobOnS3Target(target.getId(), b3.getId());
        //Add to suspect table
        final SuspectBlobTape bean = BeanFactory.newBean( SuspectBlobTape.class );
        BeanCopier.copy( bean, blobsOnTape.get(0) );
        dbSupport.getServiceManager().getService( SuspectBlobTapeService.class ).create( bean );

        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);
        test.removeUnavailableBlobs(testBlobs);
        assertEquals(2, testBlobs.size(), "One blob available on S3 target");
        assertFalse(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should not be available");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b2.getId())), "blob2 should be available");

    }

    // Test with single copy which is a suspect - S3
    @Test
    public void testWithSuspectSingleCopyAzureBlobs() {
        AzureTarget target = mockDaoDriver.createAzureTarget("azureTarget");

        BlobAzureTarget blobsonAzure = mockDaoDriver.putBlobOnAzureTarget(target.getId(), b2.getId());
        mockDaoDriver.putBlobOnAzureTarget(target.getId(), b3.getId());
        //Add to suspect table
        final SuspectBlobAzureTarget bean = BeanFactory.newBean( SuspectBlobAzureTarget.class );
        BeanCopier.copy( bean, blobsonAzure );
        dbSupport.getServiceManager().getService( SuspectBlobAzureTargetService.class ).create( bean );

        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);
        test.removeUnavailableBlobs(testBlobs);
        assertEquals(2, testBlobs.size(), "One blob available on Azure target");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b2.getId())), "blob2 should  be available");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b3.getId())), "blob3 should be available");

    }

    // Test with dual copy where one is a suspect. (Both copies on S3)
    @Test
    public void testWithSuspectDualCopyS3Blobs() {
        S3Target target = mockDaoDriver.createS3Target("s3Target");
        S3Target target2 = mockDaoDriver.createS3Target("s3Target2");
        BlobS3Target b1Target =  mockDaoDriver.putBlobOnS3Target(target.getId(), b1.getId());
        mockDaoDriver.putBlobOnS3Target(target2.getId(), b1.getId());
        mockDaoDriver.putBlobOnS3Target(target.getId(), b2.getId());
        mockDaoDriver.putBlobOnS3Target(target.getId(), b3.getId());
        //Add to suspect table
        final SuspectBlobS3Target bean = BeanFactory.newBean( SuspectBlobS3Target.class );
        BeanCopier.copy( bean, b1Target );
        dbSupport.getServiceManager().getService( SuspectBlobS3TargetService.class ).create( bean );

        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);
        test.removeUnavailableBlobs(testBlobs);
        assertEquals(3, testBlobs.size(), "One blob available on S3 target");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should be available");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b2.getId())), "blob2 should be available");

    }

    // Test with dual copy where one is a suspect. (One copy on tape and other on S3)
    @Test
    public void testWithSuspectDualCopyS3Suspect() {
        S3Target target = mockDaoDriver.createS3Target("s3Target");
        BlobS3Target b1Target =  mockDaoDriver.putBlobOnS3Target(target.getId(), b1.getId());
        mockDaoDriver.putBlobsOnTape(t1.getId(), b1);
        mockDaoDriver.putBlobOnS3Target(target.getId(), b2.getId());
        mockDaoDriver.putBlobOnS3Target(target.getId(), b3.getId());
        //Add to suspect table
        final SuspectBlobS3Target bean = BeanFactory.newBean( SuspectBlobS3Target.class );
        BeanCopier.copy( bean, b1Target );
        dbSupport.getServiceManager().getService( SuspectBlobS3TargetService.class ).create( bean );

        MockDs3ConnectionFactory mockConnection = new MockDs3ConnectionFactory();
        IomDriverImpl test = new IomDriverImpl(serviceManager,
                InterfaceProxyFactory.getProxy(DataPlannerResource.class, null),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobProgressManager.class, null),
                mockConnection,
                1000);
        test.removeUnavailableBlobs(testBlobs);
        assertEquals(3, testBlobs.size(), "One blob available on S3 target");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b1.getId())), "blob1 should be available");
        assertTrue(testBlobs.stream().anyMatch(b -> b.getId().equals(b2.getId())), "blob2 should be available");

    }

    private static DatabaseSupport dbSupport;
    private static MockDiskManager cacheManager;
    private MockDaoDriver mockDaoDriver;
    private BeansServiceManager serviceManager;
    private  Bucket bucket;
    private S3Object o1;
    private  Blob b1 ;
    private S3Object o2;
    private  Blob b2 ;
    private S3Object o3;
    private  Blob b3 ;
    private HashSet<Blob> testBlobs = new HashSet<>();
    private TapeLibrary library;
    private TapePartition partition;
    private StorageDomain sd;
    private StorageDomainMember sdm;
    private StorageDomain sd2;
    private StorageDomainMember sdm2;
    private Tape t1;
    private Tape t2;
    private Pool archivePool;
    private Pool enterprisePool;

    @BeforeAll
    public static void setDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        cacheManager = new MockDiskManager(dbSupport.getServiceManager());
    }

    @AfterEach
    public void tearDown() {
        dbSupport.reset();
    }

    @BeforeEach
    public  void dbSetUp() {
        mockDaoDriver = new MockDaoDriver( dbSupport );
        serviceManager = dbSupport.getServiceManager();
        bucket = mockDaoDriver.createBucket( null, "bucket1" );
        o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        b1 = mockDaoDriver.getBlobFor( o1.getId() );
        o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        b2 = mockDaoDriver.getBlobFor( o2.getId() );
        o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        b3 = mockDaoDriver.getBlobFor( o3.getId() );
        testBlobs = new HashSet<>(Arrays.asList(b1, b2, b3));
        mockDaoDriver.createUser("Administrator");
        initializeTapeEnvironment();
    }

    private void initializeTapeEnvironment() {
        // Initialize Library
        library = BeanFactory.newBean(TapeLibrary.class)
                .setSerialNumber("sn")
                .setName("name")
                .setManagementUrl("url");
        dbSupport.getDataManager().createBean(library);

        // Initialize Partition
        partition = BeanFactory.newBean(TapePartition.class)
                .setName("myPartition")
                .setSerialNumber("a")
                .setQuiesced(Quiesced.NO)
                .setLibraryId(library.getId())
                .setImportExportConfiguration(ImportExportConfiguration.values()[0]);
        dbSupport.getDataManager().createBean(partition);

        // Initialize Storage Domain and Member
        sd = mockDaoDriver.createStorageDomain("sd1");
        sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition.getId(),
                TapeType.values()[0]);

        sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition.getId(), TapeType.values()[ 0 ] );

        // Initialize Tapes
        t1 = (Tape)BeanFactory.newBean(Tape.class)
                .setPartitionId(partition.getId())
                .setStorageDomainMemberId(sdm.getId())
                .setState(TapeState.NORMAL)
                .setType(TapeType.values()[0])
                .setBarCode("11")
                .setId(UUID.fromString("af2992c4-8a60-4842-b915-60f9f6cb4af2"));
        dbSupport.getDataManager().createBean(t1);

        t2 = (Tape)BeanFactory.newBean(Tape.class)
                .setPartitionId(partition.getId())
                .setStorageDomainMemberId(sdm.getId())
                .setState(TapeState.NORMAL)
                .setType(TapeType.values()[0])
                .setBarCode("22")
                .setId(UUID.fromString("211aad0a-a346-11e3-9368-002590c1177c"));
        dbSupport.getDataManager().createBean(t2);

        archivePool = getArchivePool();
        dbSupport.getDataManager().createBean( archivePool );

        enterprisePool = getEnterprisePool();
        dbSupport.getDataManager().createBean( enterprisePool );
    }
    private Pool getArchivePool() {
        return BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setType( PoolType.NEARLINE )
                .setAvailableCapacity( 10000  )
                .setUsedCapacity(  20000  )
                .setHealth( PoolHealth.OK )
                .setLastAccessed( new Date() )
                .setLastModified( new Date() )
                .setLastVerified( new Date() )
                .setMountpoint( "/foo" )
                .setName(  "foo" )
                .setPoweredOn( true )
                .setState( PoolState.NORMAL );
    }

    private Pool getEnterprisePool() {
        return BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setType( PoolType.ONLINE )
                .setAvailableCapacity( 10000 )
                .setUsedCapacity(  20000  )
                .setHealth( PoolHealth.OK )
                .setLastAccessed( new Date() )
                .setLastModified( new Date() )
                .setLastVerified( new Date() )
                .setMountpoint( "/foo1" )
                .setName(  "foo1" )
                .setPoweredOn( true )
                .setState( PoolState.NORMAL );
    }
}
