/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.dataorder;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.platform.notification.domain.event.BucketNotificationEvent;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.frontend.api.JobEntryGrouping;
import com.spectralogic.s3.dataplanner.frontend.dataorder.MockDs3TargetBlobPhysicalPlacement.Ds3TargetStateBuilder;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;


import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public final class GetByPhysicalPlacementDataOrderingStrategy_Test
{
    private static DatabaseSupport dbSupport;
    private MockDaoDriver mockDaoDriver;
    private BeansServiceManager bsm;
    private MockDiskManager cacheManager;
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

    @BeforeEach
    public void setUp() {
        initializeBasicInfrastructure();
        initializeTapeEnvironment();

    }

    @BeforeAll
    public static void setupDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }
    private void initializeBasicInfrastructure() {
        mockDaoDriver = new MockDaoDriver(dbSupport);
        bsm = dbSupport.getServiceManager();
        cacheManager = new MockDiskManager(dbSupport.getServiceManager());
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

    @AfterEach
    public void tearDown() {
        dbSupport.reset();
    }


    @Test
    public void testOrderForGetJobWhenBlobsEjectedSingleCopyReportsUsefulError()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.updateBean(
                t2.setState( TapeState.EJECT_FROM_EE_PENDING ),
                Tape.STATE );
        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );
        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            getStrategy(mockDaoDriver, CollectionFactory.toSet(b1
                            .getId(), b2.getId(), b3.getId(), b4.get(0).getId(), b4.get(1)
                            .getId(), b4.get(2).getId(), b5.getId()), bsm, cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    mockDaoDriver.createUser("testUser").getId());
        });

        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> !(data.getArgs().get(0) instanceof BucketNotificationEvent)), "Shoulda generated a job creation failure.");
        assertTrue(t.getMessage().contains( "Some of the data requested is offline" ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( "; OR tapes " ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( t2.getBarCode() ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( t1.getBarCode() ), "Exception message shoulda been accurate and useful.");
    }


    @Test
    public void testOrderForGetJobWhenBlobsEjectedMultipleCopiesReportsUsefulError()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.updateBean(
                t2.setState( TapeState.LOST ),
                Tape.STATE );
        final Tape t3 = (Tape)BeanFactory.newBean( Tape.class ).setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setType( TapeType.values()[ 0 ] ).setState( TapeState.EJECTED )
                .setBarCode( "33" ).setId( UUID.randomUUID() );
        dbSupport.getDataManager().createBean( t3 );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t3.getId(), b2, b1);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                            .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                            .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    mockDaoDriver.createUser("testUser").getId() );
        });

        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> !(data.getArgs().get(0) instanceof BucketNotificationEvent)), "Shoulda generated a job creation failure.");
        assertTrue(t.getMessage().contains( "Some of the data requested is offline" ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( "; OR " ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( t2.getBarCode() ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( t3.getBarCode() ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( t1.getBarCode() ), "Exception message shoulda been accurate and useful.");
    }


    @Test
    public void testOrderForGetJobWhenBlobsEjectedMultipleIncompleteCopiesReportsUsefulError()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );
        mockDaoDriver.updateBean(
                t2.setState( TapeState.LOST ),
                Tape.STATE );


        final Tape t3 = (Tape)BeanFactory.newBean( Tape.class ).setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setType( TapeType.values()[ 0 ] ).setState( TapeState.EJECTED )
                .setBarCode( "33" ).setId( UUID.randomUUID() );
        dbSupport.getDataManager().createBean( t3 );

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t2.getId() ).setOrderIndex( 1 ).setBlobId( b1.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 1 ).setBlobId( b2.getId() ) );
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                            .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                            .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    mockDaoDriver.createUser("testUser").getId() );
        });


        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> !(data.getArgs().get(0) instanceof BucketNotificationEvent)), "Shoulda generated a job creation failure.");
        assertTrue(t.getMessage().contains( "Some of the data requested is offline" ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( "; OR tapes " ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( t2.getBarCode() ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( t3.getBarCode() ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( t1.getBarCode() ), "Exception message shoulda been accurate and useful.");
    }

    @Test
    public void testNonFullyUploadedBlobsCannotBeUsedByGetJob()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                            .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                            .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    mockDaoDriver.createUser("testUser").getId() );
        });

        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> !(data.getArgs().get(0) instanceof BucketNotificationEvent)), "Should notta generated a job creation failure.");
        assertTrue(t.getMessage().contains( b3.getId().toString() ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( b2.getId().toString() ), "Exception message shoulda been accurate and useful.");
    }


    @Test
    public void testSuspectTapeBlobsCannotBeUsedByGetJob()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        mockDaoDriver.updateBean( o3.setCreationDate( new Date() ), S3Object.CREATION_DATE );
        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() ) );
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                            .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                            .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    mockDaoDriver.createUser("testUser").getId() );
        });

        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> !(data.getArgs().get(0) instanceof BucketNotificationEvent)), "Should notta generated a job creation failure.");
        assertTrue(t.getMessage().contains( "suspect" ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( b3.getId().toString() ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( b2.getId().toString() ), "Exception message shoulda been accurate and useful.");
    }


    @Test
    public void testFailureGeneratedWhenIsIomRequest()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        mockDaoDriver.updateBean( o3.setCreationDate( new Date() ), S3Object.CREATION_DATE );
        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() ) );
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                            .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                            .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    mockDaoDriver.createUser("testUser").getId(),
                    true);
        });

        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> !(data.getArgs().get(0) instanceof BucketNotificationEvent)), "Shoulda generated a job creation failure.");
        assertTrue(t.getMessage().contains( "suspect" ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( b3.getId().toString() ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( b2.getId().toString() ), "Exception message shoulda been accurate and useful.");
    }

    @Test
    public void testSuspectPoolBlobsCannotBeUsedByGetJob()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        mockDaoDriver.updateBean( o3.setCreationDate( new Date() ), S3Object.CREATION_DATE );
        cacheManager.blobLoadedToCache( b5.getId() );

        final Pool pool = mockDaoDriver.createPool();
        final StorageDomainMember sdm2 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(), pool.getPartitionId() );
        mockDaoDriver.updateBean(
                pool.setStorageDomainMemberId( sdm2.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool.getId(), b3.getId() ) );
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                            .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                            .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    mockDaoDriver.createUser("testUser").getId() );
        });

        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> !(data.getArgs().get(0) instanceof BucketNotificationEvent)), "Should notta generated a job creation failure.");
        assertTrue(t.getMessage().contains( "suspect" ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( b3.getId().toString() ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( b2.getId().toString() ), "Exception message shoulda been accurate and useful.");
    }

    @Test
    public void testLostBlobsCannotBeUsedByGetJob()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        mockDaoDriver.updateBean( o3.setCreationDate( new Date() ), S3Object.CREATION_DATE );
        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                            .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                            .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    mockDaoDriver.createUser("testUser").getId() );
        });

        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> !(data.getArgs().get(0) instanceof BucketNotificationEvent)), "Should notta generated a job creation failure.");
        assertTrue(t.getMessage().contains( "blobs cannot be found anywhere" ), "Exception message shoulda been accurate and useful.");
        assertTrue(t.getMessage().contains( b3.getId().toString() ), "Exception message shoulda been accurate and useful.");
        assertFalse(t.getMessage().contains( b2.getId().toString() ), "Exception message shoulda been accurate and useful.");
    }

    @Test
    public void testServicingOnlyAllowedFromOnlineTapePartitionsThatAreNotQuiesced()
    {
        final UUID userId = mockDaoDriver.createUser("testUser").getId();

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setUnavailableMediaPolicy(
                        UnavailableMediaUsagePolicy.DISALLOW ),
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY );

        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        mockDaoDriver.createBlobs( o6.getId(), 1, 0 );

        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        TestUtil.assertThrows( null, AWSFailure.MEDIA_ONLINING_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                                .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                                .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                        JobRequestType.GET,
                        JobChunkClientProcessingOrderGuarantee.NONE,
                        new MockDs3TargetBlobPhysicalPlacement(),
                        userId );
            }
        } );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setState( TapePartitionState.OFFLINE ),
                TapePartition.STATE );

        TestUtil.assertThrows( null, AWSFailure.MEDIA_ONLINING_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                                .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                                .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                        JobRequestType.GET,
                        JobChunkClientProcessingOrderGuarantee.NONE,
                        new MockDs3TargetBlobPhysicalPlacement(),
                        userId );
            }
        } );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        TestUtil.assertThrows( null, AWSFailure.MEDIA_ONLINING_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                                .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                                .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                        JobRequestType.GET,
                        JobChunkClientProcessingOrderGuarantee.NONE,
                        new MockDs3TargetBlobPhysicalPlacement(),
                        userId );
            }
        } );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setState( TapePartitionState.ONLINE ),
                TapePartition.STATE );

        getStrategy(mockDaoDriver, CollectionFactory.toSet( b1
                        .getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(), b4.get( 1 )
                        .getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                userId );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.YES ),
                TapePartition.QUIESCED );

        TestUtil.assertThrows( null, AWSFailure.MEDIA_ONLINING_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                getStrategy(mockDaoDriver,
                        CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                        JobRequestType.GET,
                        JobChunkClientProcessingOrderGuarantee.NONE,
                        new MockDs3TargetBlobPhysicalPlacement(),
                        userId );
            }
        } );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setUnavailableMediaPolicy(
                        UnavailableMediaUsagePolicy.ALLOW ),
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY );

        getStrategy(mockDaoDriver,
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                userId );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setUnavailableMediaPolicy(
                        UnavailableMediaUsagePolicy.DISCOURAGED ),
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY );

        getStrategy(mockDaoDriver,
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                userId );
    }


    @Test
    public void testChunkOrderTapes()
    {
        // Only tapes are used as read source.
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );


        final List<BlobTape> bt1 =  mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2,b4.get(0), b3);
        final List<BlobTape> bt2 = mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(1), b4.get(2), b5, b1);

        int index = 0;
        for (int i = index; i < bt1.size(); i++) {
            mockDaoDriver.updateBean(bt1.get(i).setOrderIndex(i), BlobTape.ORDER_INDEX);
        }

        for (int i = index; i < bt2.size(); i++) {
            mockDaoDriver.updateBean(bt2.get(i).setOrderIndex(i), BlobTape.ORDER_INDEX);
            System.out.println("Setting order index " + i + " for blob tape " + bt2.get(i).getBlobId());

        }

        mockDaoDriver.unquiesceAllTapePartitions();

        final Map<UUID, JobEntry> jobEntries = new HashMap<>();

        int i = 0;
        Set<UUID> blobSet =  CollectionFactory.toSet(b1
                .getId(), b2.getId(), b3.getId(), b4.get(0).getId(), b4.get(1)
                .getId(), b4.get(2).getId(), b5.getId());
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(i++);
            jobEntries.put(blobId, entry);
        }

        final Set<PersistenceType> strategy = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();

        assertEquals(0,jobEntries.get(b4.get(1).getId()).getChunkNumber() );
        assertEquals(t1.getId(),jobEntries.get(b4.get(1).getId()).getReadFromTapeId() );
        assertEquals(2,jobEntries.get(b5.getId()).getChunkNumber() );
        assertEquals(6,jobEntries.get(b3.getId()).getChunkNumber() );
        assertEquals(t1.getId(),jobEntries.get(b5.getId()).getReadFromTapeId());
        assertEquals(t1.getId(),jobEntries.get(b1.getId()).getReadFromTapeId() );
    }

    @Test
    public void testBlobInCacheReadSources()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        final List<BlobTape> bt1 =  mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2,b4.get(0), b3);
        final List<BlobTape> bt2 = mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(1), b4.get(2), b5);

        int index = 0;
        for (int i = index; i < bt1.size(); i++) {
            mockDaoDriver.updateBean(bt1.get(i).setOrderIndex(i), BlobTape.ORDER_INDEX);
            System.out.println("Setting order index " + i + " for blob tape " + bt1.get(i).getBlobId());
        }

        for (int i = index; i < bt2.size(); i++) {
            mockDaoDriver.updateBean(bt2.get(i).setOrderIndex(i), BlobTape.ORDER_INDEX);
            System.out.println("Setting order index " + i + " for blob tape " + bt2.get(i).getBlobId());

        }

        mockDaoDriver.unquiesceAllTapePartitions();

        final Map<UUID, JobEntry> jobEntries = new HashMap<>();

        int i = 0;
        Set<UUID> blobSet =  CollectionFactory.toSet(b1
                .getId(), b2.getId(), b3.getId(), b4.get(0).getId(), b4.get(1)
                .getId(), b4.get(2).getId(), b5.getId());
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(i++);
            jobEntries.put(blobId, entry);
        }

        new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();

        assertEquals(0,jobEntries.get(b5.getId()).getChunkNumber() );
        assertNull(jobEntries.get(b5.getId()).getReadFromTapeId());
        assertEquals(1,jobEntries.get(b4.get(1).getId()).getChunkNumber() );
        assertEquals(6,jobEntries.get(b3.getId()).getChunkNumber() );
        assertEquals(t2.getId(),jobEntries.get(b4.get(0).getId()).getReadFromTapeId());
    }

    @Test
    public void testObsoleteBlobTapesNotUsedByGetJob()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final DatabasePersistable t1 = mockDaoDriver.createTape(partition.getId(), TapeState.NORMAL);
        final DatabasePersistable t2 = mockDaoDriver.createTape(partition.getId(), TapeState.NORMAL);
        final DatabasePersistable t3 = mockDaoDriver.createTape(partition.getId(), TapeState.NORMAL);

        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        final BlobTape bt1 = mockDaoDriver.putBlobOnTapeForStorageDomain(t1.getId(), b1.getId(), sd.getId());
        final BlobTape bt2 = mockDaoDriver.putBlobOnTapeForStorageDomain(t2.getId(), b1.getId(), sd.getId());
        final BlobTape bt3 = mockDaoDriver.putBlobOnTapeForStorageDomain(t3.getId(), b1.getId(), sd.getId());

        final BlobTape bt4 =mockDaoDriver.putBlobOnTape(t1.getId(),  b2.getId());
        mockDaoDriver.updateBean(bt1.setOrderIndex(1), BlobTape.ORDER_INDEX);
        mockDaoDriver.updateBean(bt2.setOrderIndex(2), BlobTape.ORDER_INDEX);
        mockDaoDriver.updateBean(bt3.setOrderIndex(3), BlobTape.ORDER_INDEX);
        mockDaoDriver.updateBean(bt4.setOrderIndex(4), BlobTape.ORDER_INDEX);
        mockDaoDriver.unquiesceAllTapePartitions();
        final Obsoletion obsoletion = BeanFactory.newBean( Obsoletion.class );
        final Obsoletion obsoletion2 = BeanFactory.newBean( Obsoletion.class );
        mockDaoDriver.create(obsoletion);
        mockDaoDriver.create(obsoletion2);
        transaction.getService(BlobTapeService.class).obsoleteBlobTapes(CollectionFactory.toSet(bt1), obsoletion.getId());
        transaction.getService(BlobTapeService.class).obsoleteBlobTapes(CollectionFactory.toSet(bt3), obsoletion2.getId());

        transaction.commitTransaction();

        int index = 0;
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet =  CollectionFactory.toSet(b1
                .getId(), b2.getId());
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index);
            jobEntries.put(blobId, entry);
        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();

        assertEquals(1,  types.size(), "Shoulda grouped blob into tape.");
        assertEquals(t2.getId(), jobEntries.get(b1.getId()).getReadFromTapeId());
        PersistenceType tapeType = types.iterator().next();
        assertEquals("TAPE",  tapeType.name(), "Shoulda grouped blob into tape.");
        assertEquals(t1.getId(), jobEntries.get(b2.getId()).getReadFromTapeId());

        BeansServiceManager transaction2 = dbSupport.getServiceManager().startTransaction();
        transaction2.getService(BlobTapeService.class).obsoleteBlobTapes(CollectionFactory.toSet(bt4), obsoletion.getId());
        transaction2.commitTransaction();

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            new GetByPhysicalPlacementDataOrderingStrategy(
                    jobEntries,
                    bsm,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    cacheManager,
                    "TEST_USER",
                    false
            ).setReadSources();
        });

        assertTrue(t.getMessage().contains( "Some of the data requested is offline" ), "Exception message shoulda been accurate and useful.");
    }


    @Test
    public void testZeroLengthObjectsAreAlwaysOrderedFirstForGetJobs()
    {
        //Check ordering of objects : zero length objects, objects in cache and tape.
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );

        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        int index = 0;
        Set<UUID> blobSet =  CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);
        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();

        assertEquals(0,jobEntries.get(b6.getId()).getChunkNumber() , "Shoulda ordered zero-length object ahead of all others.");
        assertNull(jobEntries.get(b6.getId()).getReadFromPoolId(), "Zero-length objects don't require a read-from source.");
        assertNull(jobEntries.get(b6.getId()).getReadFromTapeId(), "Zero-length objects don't require a read-from source.");

        assertEquals(1,jobEntries.get(b5.getId()).getChunkNumber() );
        assertNull(jobEntries.get(b5.getId()).getReadFromPoolId(), "Objects in cache don't require a read-from source.");
        assertNull(jobEntries.get(b5.getId()).getReadFromTapeId(), "Objects in cache don't require a read-from source.");
        assertEquals(t1.getId(),jobEntries.get(b4.get(0).getId()).getReadFromTapeId() );
        assertEquals(1,  types.size(), "Shoulda grouped blobs into correct destinations.");

    }


    @Test
    public void testObjectsAreOrderedByTapePlacementForGetJobsWhenStorageDomainNonEjectable()
    {
        final Tape t3 = (Tape)BeanFactory.newBean( Tape.class ).setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setType( TapeType.values()[ 0 ] ).setState( TapeState.NORMAL )
                .setBarCode( "33" ).setId( UUID.randomUUID() );
        dbSupport.getDataManager().createBean( t3 );

        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.updateBean(
                sd.setMediaEjectionAllowed( true ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );

        mockDaoDriver.updateBean(
                sd2.setMediaEjectionAllowed( false ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );

        mockDaoDriver.putBlobsOnTape(t3.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        int index = 0;
        Set<UUID> blobSet = CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);
        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();


        assertEquals(1,  types.size(), "Shoulda grouped blobs into correct groups.");
        assertEquals(0,jobEntries.get(b5.getId()).getChunkNumber() , "Shoulda ordered zero-length object ahead of all others.");
        assertNull(jobEntries.get(b5.getId()).getReadFromPoolId(), "Zero-length objects don't require a read-from source.");
        assertNull(jobEntries.get(b5.getId()).getReadFromTapeId(), "Zero-length objects don't require a read-from source.");

        //search of searchOnEjectableMedia = false should be ordered before searchOnEjectableMedia = true ( t3 before t1)
        assertEquals(jobEntries.get(b3.getId()).getChunkNumber() > jobEntries.get(b1.getId()).getChunkNumber(), true);
    }





    public void disabled_testOnlineMediaPreferredOverUnavailableMediaWhenUnavailableMediaPolicyDiscouragesUnavailable()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansServiceManager bsm = dbSupport.getServiceManager();
        final DiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setUnavailableMediaPolicy(
                        UnavailableMediaUsagePolicy.DISCOURAGED ),
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY );

        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setSerialNumber( "sn" ).setName( "name" ).setManagementUrl( "url" );
        dbSupport.getDataManager().createBean( library );

        final TapePartition partition1 = BeanFactory.newBean( TapePartition.class )
                .setName( "myPartition1" ).setSerialNumber( "a" ).setLibraryId( library.getId() )
                .setImportExportConfiguration( ImportExportConfiguration.values()[ 0 ] )
                .setQuiesced( Quiesced.NO );
        dbSupport.getDataManager().createBean( partition1 );

        final TapePartition partition2 = BeanFactory.newBean( TapePartition.class )
                .setName( "myPartition2" ).setSerialNumber( "b" ).setLibraryId( library.getId() )
                .setImportExportConfiguration( ImportExportConfiguration.values()[ 0 ] )
                .setQuiesced( Quiesced.YES );
        dbSupport.getDataManager().createBean( partition2 );

        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), partition1.getId(), TapeType.values()[ 0 ] );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition2.getId(), TapeType.values()[ 0 ] );
        mockDaoDriver.updateBean(
                sd1.setMediaEjectionAllowed( true ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        mockDaoDriver.updateBean(
                sd2.setMediaEjectionAllowed( false ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        final Tape t1 = (Tape)BeanFactory.newBean( Tape.class ).setPartitionId( partition1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "11" ).setId( UUID.fromString( "af2992c4-8a60-4842-b915-60f9f6cb4af2" ) );
        dbSupport.getDataManager().createBean( t1 );
        final DatabasePersistable t2 = BeanFactory.newBean( Tape.class ).setPartitionId( partition1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "22" ).setId( UUID.fromString( "211aad0a-a346-11e3-9368-002590c1177c" ) );
        dbSupport.getDataManager().createBean( t2 );
        final Tape t3 = BeanFactory.newBean( Tape.class ).setPartitionId( partition2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "111" );
        dbSupport.getDataManager().createBean( t3 );
        final Tape t4 = BeanFactory.newBean( Tape.class ).setPartitionId( partition2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "222" );
        dbSupport.getDataManager().createBean( t4 );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t4.getId() ).setOrderIndex( 1 ).setBlobId( b1.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t4.getId() ).setOrderIndex( 2 ).setBlobId( b2.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 2 ).setBlobId( b3.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 1 ).setBlobId( b4.get( 0 ).getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 3 ).setBlobId( b4.get( 1 ).getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 4 ).setBlobId( b4.get( 2 ).getId() ) );

        final Set<PersistenceType> groupings = getStrategy(mockDaoDriver,
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                        b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                mockDaoDriver.createUser("testUser").getId() );
        assertEquals(3,  groupings.size(), "Shoulda grouped blobs into correct groups.");
        int index = -1;

      /*  assertEquals(1,  groupings.get(++index).getEntries().size(), "Shoulda ordered object that isn't on tape first, since it should be in the cache.");
        assertEquals(null, groupings.get( index ).getEntries().iterator().next().getReadFromPoolId(), "Objects in cache don't require a read-from source.");
        assertEquals(null, groupings.get( index ).getEntries().iterator().next().getReadFromTapeId(), "Objects in cache don't require a read-from source.");
        final Object expected8 = b5.getId();
        assertEquals(expected8, groupings.get( index ).getEntries().get( 0 ).getBlobId(), "Shoulda ordered object that isn't on tape first, since it should be in the cache.");

        assertEquals(4,  groupings.get(++index).getEntries().size(), "Shoulda separated objects in cache from objects on tape.");
        assertEquals(null, groupings.get( index ).getEntries().iterator().next().getReadFromPoolId(), "Objects on tape require a read-from source.");
        final Object expected7 = t1.getId();
        assertEquals(expected7, groupings.get( index ).getEntries().iterator().next().getReadFromTapeId(), "Objects on tape require a read-from source.");
        final Object expected6 = b4.get( 0 ).getId();
        assertEquals(expected6, groupings.get( index ).getEntries().get( 0 ).getBlobId(), "Shoulda ordered objects.");
        final Object expected5 = b3.getId();
        assertEquals(expected5, groupings.get( index ).getEntries().get( 1 ).getBlobId(), "Shoulda ordered objects.");
        final Object expected4 = b4.get( 1 ).getId();
        assertEquals(expected4, groupings.get( index ).getEntries().get( 2 ).getBlobId(), "Shoulda ordered objects.");
        final Object expected3 = b4.get( 2 ).getId();
        assertEquals(expected3, groupings.get( index ).getEntries().get( 3 ).getBlobId(), "Shoulda ordered objects.");

        assertEquals(2,  groupings.get(++index).getEntries().size(), "Shoulda separated objects on different tapes from each other.");
        assertEquals(null, groupings.get( index ).getEntries().iterator().next().getReadFromPoolId(), "Objects on tape require a read-from source.");
        final Object expected2 = t2.getId();
        assertEquals(expected2, groupings.get( index ).getEntries().iterator().next().getReadFromTapeId(), "Objects on tape require a read-from source.");
        final Object expected1 = b1.getId();
        assertEquals(expected1, groupings.get( index ).getEntries().get( 0 ).getBlobId(), "Shoulda ordered objects.");
        final Object expected = b2.getId();
        assertEquals(expected, groupings.get( index ).getEntries().get( 1 ).getBlobId(), "Shoulda ordered objects.");*/
    }



    public void testUnavailableMediaPreferredOverAvailableMediaWhenUnavailableMediaPolicyAllowsUnavailable()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansServiceManager bsm = dbSupport.getServiceManager();
        final DiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setUnavailableMediaPolicy(
                        UnavailableMediaUsagePolicy.ALLOW ),
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY );

        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setSerialNumber( "sn" ).setName( "name" ).setManagementUrl( "url" );
        dbSupport.getDataManager().createBean( library );

        final TapePartition partition1 = BeanFactory.newBean( TapePartition.class )
                .setName( "myPartition1" ).setSerialNumber( "a" ).setLibraryId( library.getId() )
                .setImportExportConfiguration( ImportExportConfiguration.values()[ 0 ] )
                .setQuiesced( Quiesced.NO );
        dbSupport.getDataManager().createBean( partition1 );

        final TapePartition partition2 = BeanFactory.newBean( TapePartition.class )
                .setName( "myPartition2" ).setSerialNumber( "b" ).setLibraryId( library.getId() )
                .setImportExportConfiguration( ImportExportConfiguration.values()[ 0 ] )
                .setQuiesced( Quiesced.YES );
        dbSupport.getDataManager().createBean( partition2 );

        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), partition1.getId(), TapeType.values()[ 0 ] );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition2.getId(), TapeType.values()[ 0 ] );
        mockDaoDriver.updateBean(
                sd1.setMediaEjectionAllowed( false ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        mockDaoDriver.updateBean(
                sd2.setMediaEjectionAllowed( true ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        final Tape t1 = (Tape)BeanFactory.newBean( Tape.class ).setPartitionId( partition1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "11" ).setId( UUID.fromString( "af2992c4-8a60-4842-b915-60f9f6cb4af2" ) );
        dbSupport.getDataManager().createBean( t1 );
        final DatabasePersistable t2 = BeanFactory.newBean( Tape.class ).setPartitionId( partition1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "22" ).setId( UUID.fromString( "211aad0a-a346-11e3-9368-002590c1177c" ) );
        dbSupport.getDataManager().createBean( t2 );
        final Tape t3 = BeanFactory.newBean( Tape.class ).setPartitionId( partition2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "111" );
        dbSupport.getDataManager().createBean( t3 );
        final Tape t4 = BeanFactory.newBean( Tape.class ).setPartitionId( partition2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "222" );
        dbSupport.getDataManager().createBean( t4 );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t4.getId() ).setOrderIndex( 1 ).setBlobId( b1.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t4.getId() ).setOrderIndex( 2 ).setBlobId( b2.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 2 ).setBlobId( b3.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 1 ).setBlobId( b4.get( 0 ).getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 3 ).setBlobId( b4.get( 1 ).getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 4 ).setBlobId( b4.get( 2 ).getId() ) );

        final Set<PersistenceType> groupings = getStrategy(mockDaoDriver,
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                        b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId() ), bsm, cacheManager,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                mockDaoDriver.createUser("testUser").getId() );
        assertEquals(3,  groupings.size(), "Shoulda grouped blobs into correct groups.");
        int index = -1;

        /*assertEquals(1,  groupings.get(++index).getEntries().size(), "Shoulda ordered object that isn't on tape first, since it should be in the cache.");
        assertEquals(null, groupings.get( index ).getEntries().iterator().next().getReadFromPoolId(), "Objects in cache don't require a read-from source.");
        assertEquals(null, groupings.get( index ).getEntries().iterator().next().getReadFromTapeId(), "Objects in cache don't require a read-from source.");
        final Object expected8 = b5.getId();
        assertEquals(expected8, groupings.get( index ).getEntries().get( 0 ).getBlobId(), "Shoulda ordered object that isn't on tape first, since it should be in the cache.");

        assertEquals(4,  groupings.get(++index).getEntries().size(), "Shoulda separated objects in cache from objects on tape.");
        assertEquals(null, groupings.get( index ).getEntries().iterator().next().getReadFromPoolId(), "Objects on tape require a read-from source.");
        final Object expected7 = t1.getId();
        assertEquals(expected7, groupings.get( index ).getEntries().iterator().next().getReadFromTapeId(), "Objects on tape require a read-from source.");
        final Object expected6 = b4.get( 0 ).getId();
        assertEquals(expected6, groupings.get( index ).getEntries().get( 0 ).getBlobId(), "Shoulda ordered objects.");
        final Object expected5 = b3.getId();
        assertEquals(expected5, groupings.get( index ).getEntries().get( 1 ).getBlobId(), "Shoulda ordered objects.");
        final Object expected4 = b4.get( 1 ).getId();
        assertEquals(expected4, groupings.get( index ).getEntries().get( 2 ).getBlobId(), "Shoulda ordered objects.");
        final Object expected3 = b4.get( 2 ).getId();
        assertEquals(expected3, groupings.get( index ).getEntries().get( 3 ).getBlobId(), "Shoulda ordered objects.");

        assertEquals(2,  groupings.get(++index).getEntries().size(), "Shoulda separated objects on different tapes from each other.");
        assertEquals(null, groupings.get( index ).getEntries().iterator().next().getReadFromPoolId(), "Objects on tape require a read-from source.");
        final Object expected2 = t2.getId();
        assertEquals(expected2, groupings.get( index ).getEntries().iterator().next().getReadFromTapeId(), "Objects on tape require a read-from source.");
        final Object expected1 = b1.getId();
        assertEquals(expected1, groupings.get( index ).getEntries().get( 0 ).getBlobId(), "Shoulda ordered objects.");
        final Object expected = b2.getId();
        assertEquals(expected, groupings.get( index ).getEntries().get( 1 ).getBlobId(), "Shoulda ordered objects.");*/
    }


    @Test
    public void testNonEjectableTapesPreferredOverEjectableTapes()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );


        mockDaoDriver.updateBean(
                sd.setMediaEjectionAllowed( false ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        mockDaoDriver.updateBean(
                sd2.setMediaEjectionAllowed( true ),
                StorageDomain.MEDIA_EJECTION_ALLOWED );

        final Tape t3 = BeanFactory.newBean( Tape.class ).setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "111" );
        dbSupport.getDataManager().createBean( t3 );
        final Tape t4 = BeanFactory.newBean( Tape.class ).setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setState( TapeState.NORMAL )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "222" );
        dbSupport.getDataManager().createBean( t4 );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t4.getId() ).setOrderIndex( 1 ).setBlobId( b1.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t4.getId() ).setOrderIndex( 2 ).setBlobId( b2.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 2 ).setBlobId( b3.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 1 ).setBlobId( b4.get( 0 ).getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 3 ).setBlobId( b4.get( 1 ).getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t3.getId() ).setOrderIndex( 4 ).setBlobId( b4.get( 2 ).getId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );
        int index = 0;
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet =   CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);

        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();



        assertEquals(1,  types.size(), "Shoulda ordered object that isn't on tape first, since it should be in the cache.");
        assertNull(jobEntries.get(b5.getId()).getReadFromPoolId(), "Objects in cache don't require a read-from source.");
        assertNull(jobEntries.get(b5.getId()).getReadFromTapeId(), "Objects in cache don't require a read-from source.");
        assertEquals(0, jobEntries.get(b5.getId()).getChunkNumber(), "Shoulda ordered object that isn't on tape first, since it should be in the cache.");

        assertEquals(1,  jobEntries.get(b4.get(0).getId()).getChunkNumber(), "Shoulda separated objects in cache from objects on tape.");
        assertEquals(5,  jobEntries.get(b1.getId()).getChunkNumber(), "Shoulda preferred NonEjectableTapes over EjectableTapes.");

    }


    @Test
    public void testServicingOnlyAllowedFromNormalPoolsThatAreNotQuiesced()
    {
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setUnavailableMediaPolicy(
                        UnavailableMediaUsagePolicy.DISALLOW ),
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY );

        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final UUID userId = mockDaoDriver.createUser("testUser").getId();


        final Pool p1 = mockDaoDriver.createPool();
        final StorageDomainMember sdm1a = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(), p1.getPartitionId() );
        mockDaoDriver.updateBean(
                p1.setStorageDomainMemberId( sdm1a.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Pool p2 = mockDaoDriver.createPool();
        final StorageDomainMember sdm2a = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(), p2.getPartitionId() );
        mockDaoDriver.updateBean(
                p2.setStorageDomainMemberId( sdm2a.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t1.getId() ).setOrderIndex( 1 ).setBlobId( b1.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t1.getId() ).setOrderIndex( 2 ).setBlobId( b2.getId() ) );
        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( p1.getId() ).setBucketId( bucket.getId() ).setBlobId( b2.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( p1.getId() ).setBucketId( bucket.getId() ).setBlobId( b3.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( p2.getId() ).setBucketId( bucket.getId() ).setBlobId( b2.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( p2.getId() ).setBucketId( bucket.getId() ).setBlobId( b3.getId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Pool.class ).setQuiesced( Quiesced.YES ),
                Pool.QUIESCED );

        TestUtil.assertThrows( null, AWSFailure.MEDIA_ONLINING_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                getStrategy(mockDaoDriver,
                        CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                        JobRequestType.GET,
                        JobChunkClientProcessingOrderGuarantee.NONE,
                        new MockDs3TargetBlobPhysicalPlacement(),
                        userId );
            }
        } );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Pool.class ).setState( PoolState.LOST ),
                Pool.STATE );

        TestUtil.assertThrows( null, AWSFailure.MEDIA_ONLINING_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                getStrategy(mockDaoDriver,
                        CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                        JobRequestType.GET,
                        JobChunkClientProcessingOrderGuarantee.NONE,
                        new MockDs3TargetBlobPhysicalPlacement(),
                        userId );
            }
        } );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Pool.class ).setQuiesced( Quiesced.NO ),
                Pool.QUIESCED );

        TestUtil.assertThrows( null, AWSFailure.MEDIA_ONLINING_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                getStrategy(mockDaoDriver,
                        CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                        JobRequestType.GET,
                        JobChunkClientProcessingOrderGuarantee.NONE,
                        new MockDs3TargetBlobPhysicalPlacement(),
                        userId );
            }
        } );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Pool.class ).setState( PoolState.NORMAL ),
                Pool.STATE );

        getStrategy(mockDaoDriver,
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                userId );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Pool.class ).setQuiesced( Quiesced.YES ),
                Pool.QUIESCED );

        TestUtil.assertThrows( null, AWSFailure.MEDIA_ONLINING_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                getStrategy(mockDaoDriver,
                        CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                        JobRequestType.GET,
                        JobChunkClientProcessingOrderGuarantee.NONE,
                        new MockDs3TargetBlobPhysicalPlacement(),
                        userId );
            }
        } );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setUnavailableMediaPolicy(
                        UnavailableMediaUsagePolicy.ALLOW ),
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY );

        getStrategy(mockDaoDriver,
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                userId );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPathBackend.class ).setUnavailableMediaPolicy(
                        UnavailableMediaUsagePolicy.DISCOURAGED ),
                DataPathBackend.UNAVAILABLE_MEDIA_POLICY );

        getStrategy(mockDaoDriver,
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ), bsm, cacheManager,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                userId );
    }


    @Test
    public void testNoMoreThanOneTapeOrPoolSelectedForReadOrVerifyOfSingleBlob()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );


        final Pool p1 = mockDaoDriver.createPool();
        final StorageDomainMember sdm1a = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(), p1.getPartitionId() );
        mockDaoDriver.updateBean(
                p1.setStorageDomainMemberId( sdm1a.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Pool p2 = mockDaoDriver.createPool();
        final StorageDomainMember sdm2a = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(), p2.getPartitionId() );
        mockDaoDriver.updateBean(
                p2.setStorageDomainMemberId( sdm2a.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t1.getId() ).setOrderIndex( 1 ).setBlobId( b1.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobTape.class )
                .setTapeId( t1.getId() ).setOrderIndex( 2 ).setBlobId( b2.getId() ) );
        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( p1.getId() ).setBucketId( bucket.getId() ).setBlobId( b2.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( p1.getId() ).setBucketId( bucket.getId() ).setBlobId( b3.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( p2.getId() ).setBucketId( bucket.getId() ).setBlobId( b2.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( p2.getId() ).setBucketId( bucket.getId() ).setBlobId( b3.getId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );
        int index = 0;
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet =  CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);

        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();


        assertEquals(2,  types.size());
        assertEquals(t1.getId(),  jobEntries.get(b1.getId()).getReadFromTapeId(), "Blob b1 should be read from tape t1.");
        assertNotNull( jobEntries.get(b2.getId()).getReadFromPoolId(), "Blob b2 should be read from pool.");
        assertNull(jobEntries.get(b2.getId()).getReadFromTapeId(), "Blob b2 should not be read from tape.");

    }

    @Test
    public void testZeroLengthObjectsErrorVerifyJobs()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c", 0 );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean(
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );


        mockDaoDriver.putBlobOnTapeAndDetermineStorageDomain( t2.getId(), b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        int index = 0;
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet =  CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b6.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);

        }

        Throwable t = assertThrows(FailureTypeObservableException.class, () -> {
            new GetByPhysicalPlacementDataOrderingStrategy(
                    jobEntries,
                    bsm,
                    JobRequestType.VERIFY,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new MockDs3TargetBlobPhysicalPlacement(),
                    cacheManager,
                    "TEST_USER",
                    false
            ).setReadSources();
        });

        assertTrue(t.getMessage().contains( "The following blobs cannot be found anywhere" ), "Exception message shoulda been accurate and useful.");
    }

    @Test
    public void testZeroLengthObjectsAreNotGivenSpecialTreatmentForVerifyJobs()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c", 0 );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );


        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean(
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );


        mockDaoDriver.putBlobOnTapeAndDetermineStorageDomain( t2.getId(), b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        int index = 0;
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet =  CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);

        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.VERIFY,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();


        assertEquals(1,  types.size(), "Shoulda grouped blobs into correct groups.");
        assertEquals(1,  jobEntries.get(b3.getId()).getChunkNumber(), "Blob b6 is zero length.");
        assertEquals(  t1.getId(), jobEntries.get(b3.getId()).getReadFromTapeId());

    }


    @Test
    public void testAllTypesOfObjectsAreOrderedCorrectlyForGetJobsWhenDs3TargetsTooLowPriorityToUse()
    {
        // The order should go zero-length -> in cache -> on enterprise pool ->
        // on archive pool -> on tape

        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );
        final S3Object o7 = mockDaoDriver.createObject( bucket.getId(), "archive" );
        final Blob b7 = mockDaoDriver.getBlobFor( o7.getId() );
        final S3Object o8 = mockDaoDriver.createObject( bucket.getId(), "enterprise" );
        final Blob b8 = mockDaoDriver.getBlobFor( o8.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( archivePool.getId() ).setBlobId( b7.getId() )
                .setBucketId( o7.getBucketId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( enterprisePool.getId() ).setBlobId( b8.getId() )
                .setBucketId( o8.getBucketId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        final UUID targetId1 = UUID.randomUUID();
        final UUID targetId2 = UUID.randomUUID();
        final MockDs3TargetBlobPhysicalPlacement ds3TargetBlobPhysicalPlacement =
                new MockDs3TargetBlobPhysicalPlacement();
        ds3TargetBlobPhysicalPlacement.add(
                targetId1,
                new Ds3TargetStateBuilder()
                        .withReadPreference( TargetReadPreferenceType.LAST_RESORT )
                        .withBlobsOnTape( b1.getId(), b3.getId(), b5.getId() ).build() );
        ds3TargetBlobPhysicalPlacement.add(
                targetId2,
                new Ds3TargetStateBuilder()
                        .withReadPreference( TargetReadPreferenceType.NEVER )
                        .withBlobsOnTape( b1.getId(), b3.getId(), b5.getId() ).build() );

        int index = 0;
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet = CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId(),
                b7.getId(), b8.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);

        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                ds3TargetBlobPhysicalPlacement,
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();


        assertEquals(2,  types.size(), "Shoulda grouped blobs into correct groups.");
        assertEquals(0,  jobEntries.get(b6.getId()).getChunkNumber(), "Shoulda ordered zero-length object ahead of all others.");
        assertNull(  jobEntries.get(b6.getId()).getReadFromTapeId());
        assertNull(  jobEntries.get(b6.getId()).getReadFromPoolId());

        assertEquals(1,  jobEntries.get(b5.getId()).getChunkNumber(), "Shoulda ordered cache objects next.");
        assertNull(  jobEntries.get(b5.getId()).getReadFromTapeId());
        assertNull(  jobEntries.get(b5.getId()).getReadFromPoolId());

        assertEquals(2,  jobEntries.get(b8.getId()).getChunkNumber(), "Shoulda ordered enterprise pool objects next.");
        assertNull(  jobEntries.get(b8.getId()).getReadFromTapeId());
        assertEquals( enterprisePool.getId(), jobEntries.get(b8.getId()).getReadFromPoolId());

        assertEquals(3,  jobEntries.get(b7.getId()).getChunkNumber(), "Shoulda ordered archive pool objects next.");
        assertNull(  jobEntries.get(b7.getId()).getReadFromTapeId());
        assertEquals( archivePool.getId(), jobEntries.get(b7.getId()).getReadFromPoolId());

        assertEquals(4,  jobEntries.get(b4.get(0).getId()).getChunkNumber(), "Shoulda ordered tape objects next.");
        assertNull(  jobEntries.get(b4.get(0).getId()).getReadFromPoolId());
        assertEquals( t1.getId(), jobEntries.get(b4.get(0).getId()).getReadFromTapeId());


        assertNull(  jobEntries.get(b1.getId()).getReadFromPoolId());
        assertEquals( t2.getId(), jobEntries.get(b1.getId()).getReadFromTapeId());

        assertNull(  jobEntries.get(b3.getId()).getReadFromPoolId());
        assertNull(jobEntries.get(b3.getId()).getReadFromDs3TargetId());
        assertEquals( t1.getId(), jobEntries.get(b3.getId()).getReadFromTapeId());

    }


    @Test
    public void testAllTypesOfObjectsAreOrderedCorrectlyForGetJobsWhenDs3TargetsThatShouldBeUsed()
    {
        // The order should go zero-length -> in cache -> on enterprise pool ->
        // on archive pool -> on tape

        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );
        final S3Object o7 = mockDaoDriver.createObject( bucket.getId(), "archive" );
        final Blob b7 = mockDaoDriver.getBlobFor( o7.getId() );
        final S3Object o8 = mockDaoDriver.createObject( bucket.getId(), "enterprise" );
        final Blob b8 = mockDaoDriver.getBlobFor( o8.getId() );


        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( archivePool.getId() ).setBlobId( b7.getId() )
                .setBucketId( o7.getBucketId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( enterprisePool.getId() ).setBlobId( b8.getId() )
                .setBucketId( o8.getBucketId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        final UUID targetId1 = UUID.randomUUID();
        final UUID targetId2 = UUID.randomUUID();
        final MockDs3TargetBlobPhysicalPlacement ds3TargetBlobPhysicalPlacement =
                new MockDs3TargetBlobPhysicalPlacement();
        ds3TargetBlobPhysicalPlacement.add(
                targetId1,
                new Ds3TargetStateBuilder()
                        .withReadPreference( TargetReadPreferenceType.AFTER_ONLINE_POOL )
                        .withBlobsOnTape( b1.getId(), b3.getId() ).build() );
        ds3TargetBlobPhysicalPlacement.add(
                targetId2,
                new Ds3TargetStateBuilder()
                        .withReadPreference( TargetReadPreferenceType.AFTER_NEARLINE_POOL )
                        .withBlobsOnTape( b1.getId(), b3.getId(), b5.getId() ).build() );

        int index = 0;
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet = CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId(),
                b7.getId(), b8.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);

        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                ds3TargetBlobPhysicalPlacement,
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();



        assertEquals(3,  types.size(), "Shoulda grouped blobs into correct groups.");

        assertNull(  jobEntries.get(b1.getId()).getReadFromPoolId());
        assertNotNull(jobEntries.get(b1.getId()).getReadFromDs3TargetId());
        assertNull( jobEntries.get(b1.getId()).getReadFromTapeId());

        assertNull(  jobEntries.get(b5.getId()).getReadFromPoolId());
        assertNotNull(jobEntries.get(b5.getId()).getReadFromDs3TargetId());
        assertNull( jobEntries.get(b5.getId()).getReadFromTapeId());

        assertEquals(jobEntries.get(b1.getId()).getChunkNumber() < jobEntries.get(b5.getId()).getChunkNumber(), true);

    }


    @Test
    public void testAllTypesOfObjectsAreOrderedCorrectlyForGetJobsWhenDs3TargetsMinimizeLatency()
    {
        // The order should go zero-length -> in cache -> on enterprise pool ->
        // on archive pool -> on tape

        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );
        final S3Object o7 = mockDaoDriver.createObject( bucket.getId(), "archive" );
        final Blob b7 = mockDaoDriver.getBlobFor( o7.getId() );
        final S3Object o8 = mockDaoDriver.createObject( bucket.getId(), "enterprise" );
        final Blob b8 = mockDaoDriver.getBlobFor( o8.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( archivePool.getId() ).setBlobId( b7.getId() )
                .setBucketId( o7.getBucketId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( enterprisePool.getId() ).setBlobId( b8.getId() )
                .setBucketId( o8.getBucketId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        final UUID targetId1 = UUID.randomUUID();
        final UUID targetId2 = UUID.randomUUID();
        final MockDs3TargetBlobPhysicalPlacement ds3TargetBlobPhysicalPlacement =
                new MockDs3TargetBlobPhysicalPlacement();
        ds3TargetBlobPhysicalPlacement.add(
                targetId1,
                new Ds3TargetStateBuilder()
                        .withReadPreference( TargetReadPreferenceType.MINIMUM_LATENCY )
                        .withBlobsOnPool( b1.getId() )
                        .withBlobsOnTape( b1.getId(), b3.getId(), b5.getId() ).build() );
        ds3TargetBlobPhysicalPlacement.add(
                targetId2,
                new Ds3TargetStateBuilder()
                        .withReadPreference( TargetReadPreferenceType.LAST_RESORT )
                        .withBlobsOnTape( b1.getId(), b3.getId(), b5.getId() ).build() );


        int index = 0;
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet = CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId(),
                b7.getId(), b8.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);

        }
        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                ds3TargetBlobPhysicalPlacement,
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();



        assertEquals(3,  types.size(), "Shoulda grouped blobs into correct groups.");

        assertNull(  jobEntries.get(b1.getId()).getReadFromPoolId());
        assertEquals(targetId1, jobEntries.get(b1.getId()).getReadFromDs3TargetId());
        assertNull( jobEntries.get(b1.getId()).getReadFromTapeId());

        assertNull(  jobEntries.get(b5.getId()).getReadFromPoolId());
        assertNotNull( jobEntries.get(b5.getId()).getReadFromDs3TargetId());
        assertNull( jobEntries.get(b5.getId()).getReadFromTapeId());
        assertEquals(jobEntries.get(b1.getId()).getChunkNumber() < jobEntries.get(b5.getId()).getChunkNumber(), true);

    }




    @Test
    public void testAllTypesOfObjectsAreOrderedCorrectlyForGetJobsWhenAzureTargetsThatShouldBeUsed()
    {
        // The order should go zero-length -> in cache -> on enterprise pool ->
        // on archive pool -> on tape
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );
        final S3Object o7 = mockDaoDriver.createObject( bucket.getId(), "archive" );
        final Blob b7 = mockDaoDriver.getBlobFor( o7.getId() );
        final S3Object o8 = mockDaoDriver.createObject( bucket.getId(), "enterprise" );
        final Blob b8 = mockDaoDriver.getBlobFor( o8.getId() );


        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( archivePool.getId() ).setBlobId( b7.getId() )
                .setBucketId( o7.getBucketId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( enterprisePool.getId() ).setBlobId( b8.getId() )
                .setBucketId( o8.getBucketId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );

        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "t1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        mockDaoDriver.updateBean(
                target1.setDefaultReadPreference( TargetReadPreferenceType.AFTER_ONLINE_POOL ),
                ReplicationTarget.DEFAULT_READ_PREFERENCE );
        mockDaoDriver.updateBean(
                target2.setDefaultReadPreference( TargetReadPreferenceType.AFTER_NEARLINE_POOL ),
                ReplicationTarget.DEFAULT_READ_PREFERENCE );
        mockDaoDriver.putBlobOnAzureTarget( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target1.getId(), b3.getId() );

        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), b5.getId() );

        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        Set<UUID> blobSet = CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId(),
                b7.getId(), b8.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId);
            jobEntries.put(blobId, entry);

        }

        final Set<PersistenceType> types = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();


        assertEquals(3,  types.size(), "Shoulda grouped blobs into correct groups.");

        final Object expectedAzureTargetBlob = b1.getId();
        assertNull(jobEntries.get(expectedAzureTargetBlob).getReadFromPoolId());
        assertNull(jobEntries.get(expectedAzureTargetBlob).getReadFromTapeId());
        assertEquals(target1.getId(), jobEntries.get(expectedAzureTargetBlob).getReadFromAzureTargetId() );

        final Object expectedAzureTargetAfterNearline = b5.getId();
        assertNull(jobEntries.get(expectedAzureTargetAfterNearline).getReadFromPoolId());
        assertNull(jobEntries.get(expectedAzureTargetAfterNearline).getReadFromTapeId());
        assertEquals(target2.getId(), jobEntries.get(expectedAzureTargetAfterNearline).getReadFromAzureTargetId() );

    }


    @Test
    public void testAllTypesOfObjectsAreOrderedCorrectlyForGetJobsWhenS3TargetsThatShouldBeUsed()
    {
        // The order should go zero-length -> in cache -> on enterprise pool ->
        // on archive pool -> on tape
        final S3Target target1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        mockDaoDriver.updateBean(
                target1.setDefaultReadPreference( TargetReadPreferenceType.AFTER_ONLINE_POOL ),
                ReplicationTarget.DEFAULT_READ_PREFERENCE );
        mockDaoDriver.updateBean(
                target2.setDefaultReadPreference( TargetReadPreferenceType.AFTER_NEARLINE_POOL ),
                ReplicationTarget.DEFAULT_READ_PREFERENCE );


        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );
        final S3Object o7 = mockDaoDriver.createObject( bucket.getId(), "archive" );
        final Blob b7 = mockDaoDriver.getBlobFor( o7.getId() );
        final S3Object o8 = mockDaoDriver.createObject( bucket.getId(), "enterprise" );
        final Blob b8 = mockDaoDriver.getBlobFor( o8.getId() );

        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( archivePool.getId() ).setBlobId( b7.getId() )
                .setBucketId( o7.getBucketId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( enterprisePool.getId() ).setBlobId( b8.getId() )
                .setBucketId( o8.getBucketId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );


        mockDaoDriver.putBlobOnS3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnS3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnS3Target( target1.getId(), b5.getId() );

        mockDaoDriver.putBlobOnS3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnS3Target( target2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnS3Target( target2.getId(), b5.getId() );

        final Map<UUID, JobEntry> jobEntries = new HashMap<>();
        int index = 0;
        Set<UUID> blobSet =  CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId(),
                b7.getId(), b8.getId() );
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId).setChunkNumber(index++);
            jobEntries.put(blobId, entry);

        }

        Set<PersistenceType> groupings = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                JobRequestType.GET,
                JobChunkClientProcessingOrderGuarantee.NONE,
                new MockDs3TargetBlobPhysicalPlacement(),
                cacheManager,
                "TEST_USER",
                false
        ).setReadSources();



        assertEquals(3,  groupings.size(), "Shoulda grouped blobs into correct groups.");

        final Object expectedZeroLengthBlob = b6.getId();
        assertEquals(0,jobEntries.get(expectedZeroLengthBlob).getChunkNumber(), "Shoulda ordered zero length object first." );
        assertNull(jobEntries.get(expectedZeroLengthBlob).getReadFromTapeId());
        assertNull(jobEntries.get(expectedZeroLengthBlob).getReadFromPoolId());

        final Object expectedCacheBlob = b5.getId();
        assertEquals(1,jobEntries.get(expectedCacheBlob).getChunkNumber(), "Shoulda ordered cache object next." );
        assertNull(jobEntries.get(expectedCacheBlob).getReadFromPoolId());
        assertNull(jobEntries.get(expectedCacheBlob).getReadFromTapeId());

        final Object expectedEnterprisePoolBlob = b8.getId();
        assertEquals(2,jobEntries.get(expectedEnterprisePoolBlob).getChunkNumber(), "Shoulda ordered enterprise blob object next." );
        assertEquals(enterprisePool.getId(), jobEntries.get(expectedEnterprisePoolBlob).getReadFromPoolId());
        assertNull(jobEntries.get(expectedEnterprisePoolBlob).getReadFromTapeId());


        final Object expectedS3TargetBlob = b1.getId();
        assertNull(jobEntries.get(expectedS3TargetBlob).getReadFromPoolId());
        assertNull(jobEntries.get(expectedS3TargetBlob).getReadFromTapeId());
        assertEquals(target1.getId(), jobEntries.get(expectedS3TargetBlob).getReadFromS3TargetId() );

        final Object expectedLastS3TargetBlob = b3.getId();
        assertNull(jobEntries.get(expectedLastS3TargetBlob).getReadFromPoolId());
        assertNull(jobEntries.get(expectedLastS3TargetBlob).getReadFromTapeId());
        assertEquals(target1.getId(), jobEntries.get(expectedLastS3TargetBlob).getReadFromS3TargetId() );

        final Object expectedArchivePoolBlob = b7.getId();
        assertEquals(5,jobEntries.get(expectedArchivePoolBlob).getChunkNumber(), "Shoulda ordered archive blob object next." );
        assertEquals(archivePool.getId(), jobEntries.get(expectedArchivePoolBlob).getReadFromPoolId());
        assertNull(jobEntries.get(expectedArchivePoolBlob).getReadFromTapeId());

        final Object expectedTape1Blob = b4.get(0).getId();
        assertEquals(6, jobEntries.get(expectedTape1Blob).getChunkNumber());
        assertEquals(t1.getId(), jobEntries.get(expectedTape1Blob).getReadFromTapeId());
        assertNull(jobEntries.get(expectedTape1Blob).getReadFromPoolId());

        final Object expectedTapeBlob = b2.getId();
        assertEquals(t2.getId(), jobEntries.get(expectedTapeBlob).getReadFromTapeId());
        assertNull(jobEntries.get(expectedTapeBlob).getReadFromPoolId());

    }

    public Set<PersistenceType> getStrategy(MockDaoDriver mockDaoDriver,
                                            Set<UUID> set,
                                            BeansServiceManager bsm,
                                            DiskManager cacheManager,
                                            JobRequestType jobRequestType,
                                            JobChunkClientProcessingOrderGuarantee jobChunkClientProcessingOrderGuarantee,
                                            Ds3TargetBlobPhysicalPlacement mockDs3TargetBlobPhysicalPlacement,
                                            UUID testUser) {
        return getStrategy(mockDaoDriver,
                set,
                bsm,
                cacheManager,
                jobRequestType,
                jobChunkClientProcessingOrderGuarantee,
                mockDs3TargetBlobPhysicalPlacement,
                testUser,
                false);
    }

    private Set<PersistenceType> getStrategy(MockDaoDriver mockDaoDriver,
                                             Set<UUID> set,
                                             BeansServiceManager bsm,
                                             DiskManager cacheManager,
                                             JobRequestType jobRequestType,
                                             JobChunkClientProcessingOrderGuarantee jobChunkClientProcessingOrderGuarantee,
                                             Ds3TargetBlobPhysicalPlacement mockDs3TargetBlobPhysicalPlacement,
                                             UUID testUser,
                                             boolean iom) {
        final Map<UUID, JobEntry> jobEntries = new HashMap<>();

        for (UUID blobId : set) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId);
            jobEntries.put(blobId, entry);
        }
        final Set<PersistenceType> strategy = new GetByPhysicalPlacementDataOrderingStrategy(
                jobEntries,
                bsm,
                jobRequestType,
                jobChunkClientProcessingOrderGuarantee,
                mockDs3TargetBlobPhysicalPlacement,
                cacheManager,
                "TEST_USER",
                iom
        ).setReadSources();
        return strategy;
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
