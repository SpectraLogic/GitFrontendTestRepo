/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobbingPolicy;
import com.spectralogic.s3.common.rpc.dataplanner.domain.S3ObjectToCreate;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public final class S3ObjectCreatorImpl_Test
{
    @Test
    public void testConstructorNullBucketIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new S3ObjectCreatorImpl(
                        PREFERRED_BLOB_SIZE,
                        MAX_BLOB_SIZE, 
                        BlobbingPolicy.ENABLED, 
                        null,
                        null, 
                        getObjectsToCreate() );
            }
        } );
    }
    
    @Test
    public void testConstructorNullObjectsToCreateNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new S3ObjectCreatorImpl(
                        PREFERRED_BLOB_SIZE,
                        MAX_BLOB_SIZE, 
                        BlobbingPolicy.ENABLED, 
                        null,
                        UUID.randomUUID(), 
                        null );
            }
        } );
    }
    
    @Test
    public void testConstructorNullBlobbingPolicyNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new S3ObjectCreatorImpl(
                        PREFERRED_BLOB_SIZE,
                        MAX_BLOB_SIZE, 
                        null,
                        null,
                        UUID.randomUUID(), 
                        getObjectsToCreate() );
            }
        } );
    }
    
    @Test
    public void testConstructorHappyConstruction()
    {
        new S3ObjectCreatorImpl( 
                PREFERRED_BLOB_SIZE,
                MAX_BLOB_SIZE, 
                BlobbingPolicy.ENABLED, 
                null,
                UUID.randomUUID(), 
                getObjectsToCreate() );
    }
    
    @Test
    public void testCannotCreateFoldersWithData()
    {
        final Set< S3ObjectToCreate > objectsToCreate = new HashSet<>();
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "f1/" ).setSizeInBytes( 10000 ) );
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "f2/" ).setSizeInBytes( 1000 ) );
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "f3/" ).setSizeInBytes( 5 * MAX_BLOB_SIZE - 1 ) );
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "f4/" ).setSizeInBytes( 5 * MAX_BLOB_SIZE ) );
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "f5/" ).setSizeInBytes( 5 * MAX_BLOB_SIZE + 1 ) );

        TestUtil.assertThrows( null, DataPlannerException.class, new BlastContainer()
        {
            public void test()
            {
                new S3ObjectCreatorImpl(
                        1000, 1000, BlobbingPolicy.ENABLED, null, UUID.randomUUID(), objectsToCreate );
            }
        } );
    }
    
    @Test
    public void testGettersReturnObjectsAndBlobsCorrectlyBeforeCommitWhenNoDataObject()
    {
        final Set< S3ObjectToCreate > objectsToCreate = new HashSet<>();
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o1" ).setSizeInBytes( 0 ) );
        
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl( 
                PREFERRED_BLOB_SIZE, 
                MAX_BLOB_SIZE,
                BlobbingPolicy.DISABLED, 
                null,
                UUID.randomUUID(), 
                objectsToCreate );
        final Set< S3Object > objects = creator.getObjects();
        assertEquals( 1,
                objects.size(),
                "Shoulda created an object for every object to create."
        );
        
        final Set< Blob > blobs = creator.getBlobs();
        assertEquals( 1,
                blobs.size(),
                "Shoulda blobbed up the objects."
                );
    }
    
    @Test
    public void testGettersReturnObjectsAndBlobsCorrectlyBeforeCommitWhenFolderObject()
    {
        final Set< S3ObjectToCreate > objectsToCreate = new HashSet<>();
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o1/" ).setSizeInBytes( 0 ) );
        
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl( 
                PREFERRED_BLOB_SIZE, 
                MAX_BLOB_SIZE,
                BlobbingPolicy.DISABLED, 
                null,
                UUID.randomUUID(), 
                objectsToCreate );
        final Set< S3Object > objects = creator.getObjects();
        assertEquals(1,
                objects.size(),
                "Shoulda created an object for every object to create."
                 );
        
        final Set< Blob > blobs = creator.getBlobs();
        assertEquals( 1,
                blobs.size(),
                "Shoulda blobbed up the objects."
                );
    }
    
    @Test
    public void testGettersReturnObjectsAndBlobsCorrectlyBeforeCommitWhenBlobbingDisabled()
    {
        final Set< S3ObjectToCreate > objectsToCreate = new HashSet<>();
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o1" ).setSizeInBytes( 10000 ) );
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o2" ).setSizeInBytes( 1000 ) );
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o3" ).setSizeInBytes( MAX_BLOB_SIZE - 1 ) );
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o4" ).setSizeInBytes( MAX_BLOB_SIZE ) );
        
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl( 
                PREFERRED_BLOB_SIZE, 
                MAX_BLOB_SIZE,
                BlobbingPolicy.DISABLED,
                null,
                UUID.randomUUID(), 
                objectsToCreate );
        final Set< S3Object > objects = creator.getObjects();
        assertEquals(4,
                objects.size(),
                "Shoulda created an object for every object to create."
                 );
        
        final Set< Blob > blobs = creator.getBlobs();
        assertEquals(4,
                blobs.size(),
                "Shoulda blobbed up the objects."
                );
    }
    
    @Test
    public void testCannotBlobObjectTooBigWhenBlobbingDisabled()
    {
        final Set< S3ObjectToCreate > objectsToCreate = getObjectsToCreate();
        objectsToCreate.iterator().next().setSizeInBytes( MAX_BLOB_SIZE + 1 );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                new S3ObjectCreatorImpl( 
                        PREFERRED_BLOB_SIZE,
                        MAX_BLOB_SIZE, 
                        BlobbingPolicy.DISABLED,
                        null,
                        UUID.randomUUID(), 
                        objectsToCreate );
            }
        } );
    }
    
    @Test
    public void testGettersReturnObjectsAndBlobsCorrectlyBeforeCommit()
    {
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl(
                PREFERRED_BLOB_SIZE,
                MAX_BLOB_SIZE,
                BlobbingPolicy.ENABLED, 
                null,
                UUID.randomUUID(), 
                getObjectsToCreate() );
        final Set< S3Object > objects = creator.getObjects();
        assertEquals( 5,
                objects.size(),
                "Shoulda created an object for every object to create."
                );
        
        final Set< Blob > blobs = creator.getBlobs();
        assertEquals(1512,
                blobs.size(),
                "Shoulda blobbed up the objects."
                 );
    }
    
    @Test
    public void testBlobsCalculatedCorrectlyWhenRightAroundLargeBlobBoundary()
    {
        final long maxBlobSize = 10L * 1024 * 1024 * 1024 * 1024;

        final Set< S3ObjectToCreate > objectsToCreate = new HashSet<>();
        objectsToCreate.add(
                BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "a" ).setSizeInBytes( maxBlobSize ) );
        objectsToCreate.add(
                BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "b" ).setSizeInBytes( maxBlobSize + 1 ) );
        objectsToCreate.add(
                BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "c" ).setSizeInBytes( maxBlobSize + 1 ) );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl( 
                maxBlobSize,
                maxBlobSize, 
                BlobbingPolicy.ENABLED, 
                null,
                bucket.getId(),
                objectsToCreate );
        
        assertEquals(  3,
                creator.getObjects().size(),
                "a shoulda had single blob; b and c shoulda had 2 blobs each."
               );
        assertEquals(  5,
                creator.getBlobs().size(),
                "a shoulda had single blob; b and c shoulda had 2 blobs each."
               );
    }
    
    @Test
    //@Tag("rpc-integration")
    public void testCommitCommitsObjectsAndBlobsToTransaction()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl( 
                PREFERRED_BLOB_SIZE,
                MAX_BLOB_SIZE, 
                BlobbingPolicy.ENABLED, 
                null,
                bucket.getId(),
                getObjectsToCreate() );
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        creator.commit( 
                transaction.getService( S3ObjectService.class ), 
                transaction.getService( BlobService.class ) );
        transaction.commitTransaction();
        
        assertEquals(5,
                dbSupport.getDataManager().getCount( S3Object.class, Require.nothing()),
                        "Shoulda created an object for every object to create."
                 );
        assertEquals(1512,
                dbSupport.getDataManager().getCount( Blob.class, Require.nothing() ),
                "Shoulda blobbed up the objects."
                 );
    }
    
    @Test
    public void testCommitWithFoldersCommitsObjectsAndBlobsToTransaction()
    {
        final Set< S3ObjectToCreate > objectsToCreate = getObjectsToCreate();
        objectsToCreate.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "f1/" ).setSizeInBytes( 0 ) );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl( 
                PREFERRED_BLOB_SIZE,
                MAX_BLOB_SIZE, 
                BlobbingPolicy.ENABLED, 
                null,
                bucket.getId(),
                objectsToCreate );
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        creator.commit( 
                transaction.getService( S3ObjectService.class ), 
                transaction.getService( BlobService.class ) );
        transaction.commitTransaction();
        
        assertEquals(6,
                dbSupport.getDataManager().getCount( S3Object.class, Require.nothing() ),
                "Shoulda created an object for every object and folder to create."
                 );
        assertEquals( 1513,
                dbSupport.getDataManager().getCount( Blob.class, Require.nothing() ),
                "Shoulda blobbed up the objects."
                );
    }
    
    @Test
    public void testCommitWhenConflictsWithLengthMismatchAndIgnoreConflictsTrueNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.createObject( bucket.getId(), "o2", 999 );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new S3ObjectCreatorImpl( 
                        PREFERRED_BLOB_SIZE,
                        MAX_BLOB_SIZE, 
                        BlobbingPolicy.ENABLED, 
                        dbSupport.getServiceManager().getService( S3ObjectService.class ),
                        bucket.getId(),
                        getObjectsToCreate() );
            }
        } );
    }
    
    @Test
    public void testCommitWhenConflictsWithoutLengthMismatchAndIgnoreConflictsFalseNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.createObject( bucket.getId(), "o2", 1000 );
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl( 
                PREFERRED_BLOB_SIZE,
                MAX_BLOB_SIZE, 
                BlobbingPolicy.ENABLED, 
                null,
                bucket.getId(),
                getObjectsToCreate() );
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        TestUtil.assertThrows( null, AWSFailure.OBJECT_ALREADY_EXISTS, new BlastContainer()
        {
            public void test() throws Throwable
            {
                creator.commit( 
                        transaction.getService( S3ObjectService.class ), 
                        transaction.getService( BlobService.class ) );
            }
        } );
        transaction.closeTransaction();
    }

    @Test
    public void testS3ObjectNamesWithConflictingNormalizedNamesSupported() {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final String name = "re\u0301sume\u0301"; //with combining accents
        final String normalizedName = "r\u00e9sum\u00e9"; //with an accent letter e
        final S3ObjectToCreate objectToCreate = BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( name ).setSizeInBytes( PREFERRED_BLOB_SIZE * 10 );
        final S3ObjectToCreate objectToCreate2 = BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( normalizedName ).setSizeInBytes( PREFERRED_BLOB_SIZE * 10 );
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl(
                PREFERRED_BLOB_SIZE,
                MAX_BLOB_SIZE,
                BlobbingPolicy.ENABLED,
                null,
                bucket.getId(),
                CollectionFactory.toSet(objectToCreate, objectToCreate2));
        final BeansServiceManager transaction =
                dbSupport.getServiceManager().startTransaction();
        creator.commit(
                transaction.getService( S3ObjectService.class ),
                transaction.getService( BlobService.class ) );
        transaction.commitTransaction();
        transaction.closeTransaction();
        final Set<String> objectNames = mockDaoDriver.retrieveAll(S3Object.class).stream().map((it) -> it.getName()).collect(Collectors.toSet());
        assertTrue(objectNames.contains(name), "One object should have had non-normalized name.");
        assertTrue(objectNames.contains(normalizedName), "One object should have had normalized name.");
        assertEquals(2, objectNames.size());
        assertNotEquals(name, normalizedName, "Name and normalized name should not be considered equal.");
    }
    
    @Test
    public void testCommitWhenConflictsWithoutLengthMismatchAndIgnoreConflictsTrueAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.createObject( bucket.getId(), "o2", 1000 );
        final S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl( 
                PREFERRED_BLOB_SIZE,
                MAX_BLOB_SIZE, 
                BlobbingPolicy.ENABLED, 
                dbSupport.getServiceManager().getService( S3ObjectService.class ),
                bucket.getId(),
                getObjectsToCreate() );
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        creator.commit( 
                transaction.getService( S3ObjectService.class ), 
                transaction.getService( BlobService.class ) );
        transaction.commitTransaction();
        
        assertEquals( 5,
                dbSupport.getDataManager().getCount( S3Object.class, Require.nothing() ),
                "Shoulda created an object for every object and folder to create."
                );
        assertEquals(1512,
                dbSupport.getDataManager().getCount( Blob.class, Require.nothing() ),
                "Shoulda blobbed up the objects."
                 );
    }
    
    
    private Set< S3ObjectToCreate > getObjectsToCreate()
    {
        final Set< S3ObjectToCreate > retval = new HashSet<>();
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o1" ).setSizeInBytes( PREFERRED_BLOB_SIZE * 10 ) );
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o2" ).setSizeInBytes( 1000 ) );
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o3" ).setSizeInBytes( 5 * MAX_BLOB_SIZE - 1 ) );
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o4" ).setSizeInBytes( 5 * MAX_BLOB_SIZE ) );
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o5" ).setSizeInBytes( 5 * MAX_BLOB_SIZE + 1 ) );
        return retval;
    }
    
    
    private final static long PREFERRED_BLOB_SIZE = 1L * 1024 * 1024 * 1024;
    private final static long MAX_BLOB_SIZE = 100L * 1024 * 1024 * 1024;
    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        // Manually truncate Blob table to avoid timeout for tests creating many blobs (1500+)
        // Truncate is fast and bypasses FK constraints since dependent tables are also cleaned
        final int blobCount = dbSupport.getDataManager().getCount(Blob.class, Require.nothing());
        if (blobCount > 1000) {
            dbSupport.getDataManager().truncate(Blob.class);
        }
        dbSupport.reset();
    }

}
