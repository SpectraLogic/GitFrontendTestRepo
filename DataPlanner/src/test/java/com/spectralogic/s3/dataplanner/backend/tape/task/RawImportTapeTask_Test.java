/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.RawImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public final class RawImportTapeTask_Test 
{
    @Test
    public void testDequeuedWhenRawImportedSuccessfullyDoesNotChangeTapeState()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final RawImportTapeDirective directive =
                mockDaoDriver.createRawImportTapeDirective( tape.getId(), bucket.getId() );
        
        final RawImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        
        task.dequeued();

        assertEquals(TapeState.NORMAL, mockDaoDriver.attain( tape ).getState(), "Should notta changed tape state.");
    }
    
    
    @Test
    public void testDequeuedWhenNotRawImportedSuccessfullyChangesTapeState()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.RAW_IMPORT_PENDING );
        final RawImportTapeDirective directive =
                mockDaoDriver.createRawImportTapeDirective( tape.getId(), bucket.getId() );
        
        final RawImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.dequeued();

        assertEquals(TapeState.LTFS_WITH_FOREIGN_DATA, mockDaoDriver.attain( tape ).getState(), "Shoulda changed tape state.");
    }
    
    
    @Test
    public void testDequeuedWhenTapeNoLongerExistsDoesNotBlowUp()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.RAW_IMPORT_PENDING );
        final RawImportTapeDirective directive =
                mockDaoDriver.createRawImportTapeDirective( tape.getId(), bucket.getId() );
        
        final RawImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        mockDaoDriver.delete( Tape.class, tape );
        task.dequeued();
    }
    
    
    @Test
    public void testRawImportWhenSinglePageOfResultsAndEverythingRawImportedIsNewWorksNoVerify()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_1" );
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_2" );
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_3" );
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_4" );
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_5" );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.update( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );
        tapeService.transistState( tape, TapeState.RAW_IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final RawImportTapeDirective directive =
                mockDaoDriver.createRawImportTapeDirective( tape.getId(), bucket.getId() );

        final BasicTestsInvocationHandler blobStoreBtih =
                new BasicTestsInvocationHandler( null );
        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( null );
        tapeDriveResource.setVerifyDataResult( BeanFactory.newBean( BlobIoFailures.class ).setFailures( 
                new BlobIoFailure [] { BeanFactory.newBean( BlobIoFailure.class ) } ) );
        final RawImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully rawImported tape.");
        assertFalse(tape.getId().equals(
                        tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId() ), "Shoulda successfully rawImported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenRawImported( dbSupport.getServiceManager() );
        assertEquals(0,  blobStoreBtih.getTotalCallCount(), "Should notta verified tape after rawImport.");
    }
    

    @Test
    public void testImportInterleavedBlobsInSingleResponseWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_0" );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.update( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );
        tapeService.transistState( tape, TapeState.RAW_IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final RawImportTapeDirective directive =
                mockDaoDriver.createRawImportTapeDirective( tape.getId(), bucket.getId() );

        final BasicTestsInvocationHandler blobStoreBtih =
                new BasicTestsInvocationHandler( null );
        final List< S3ObjectsOnMedia > responses =
                constructResponses( 1, Integer.MAX_VALUE, true, 0, 1, true);
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( null );
        tapeDriveResource.setVerifyDataResult( BeanFactory.newBean( BlobIoFailures.class ).setFailures( 
                new BlobIoFailure [] { BeanFactory.newBean( BlobIoFailure.class ) } ) );
        final RawImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully rawImported tape.");
        assertFalse(tape.getId().equals(
                        tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId() ), "Shoulda successfully rawImported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");
        assertEquals(0,  blobStoreBtih.getTotalCallCount(), "Should notta verified tape after rawImport.");
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda imported 15 objects.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.CREATION_DATE, null)), "Shoulda imported all blobs for each object.");
        assertEquals(20,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Shoulda imported 20 blobs.");
    }
    
    
    private void verifyEverythingHasBeenRawImported( final BeansServiceManager serviceManager )
    {
        verifyEverythingHasBeenRawImported( serviceManager, 20, true );
    }
    
    
    private void verifyEverythingHasBeenRawImported(
            final BeansServiceManager serviceManager,
            final int numBlobTapesExpected,
            final boolean verifyOrder )
    {
        final Bucket b2 = serviceManager.getRetriever( Bucket.class ).attain(
                Bucket.NAME, "b_2" );
        final Bucket b3 = serviceManager.getRetriever( Bucket.class ).attain( 
                Bucket.NAME, "b_3" );
        final S3Object o00 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_0blobs_0present" ); // b_1
        final S3Object o10 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_1blobs_0present" ); // b_2
        final S3Object o11 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_1blobs_1present" ); // b_2
        final S3Object o20 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_0present" ); // b_3
        final S3Object o21 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_1present" ); // b_3
        final S3Object o22 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_2present" ); // b_3
        assertEquals(2,  serviceManager.getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.BUCKET_ID, b2.getId())), "Shoulda rawImported all contents.");
        assertEquals(3,  serviceManager.getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.BUCKET_ID, b3.getId())), "Shoulda rawImported all contents.");

        assertEquals(0, serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o00.getId())), "Shoulda rawImported all contents.");
        assertEquals(0,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o10.getId())), "Shoulda rawImported all contents.");
        assertEquals(1,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o11.getId())), "Shoulda rawImported all contents.");
        assertEquals(0,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o20.getId())), "Shoulda rawImported all contents.");
        assertEquals(1,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o21.getId())), "Shoulda rawImported all contents.");
        assertEquals(2,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o22.getId())), "Shoulda rawImported all contents.");

        final Object expected2 = b3.getId();
        assertEquals(expected2, o20.getBucketId(), "Shoulda rawImported all contents.");
        final Object expected1 = b3.getId();
        assertEquals(expected1, o21.getBucketId(), "Shoulda rawImported all contents.");
        final Object expected = b3.getId();
        assertEquals(expected, o22.getBucketId(), "Shoulda rawImported all contents.");

        assertEquals(0,  o20.getCreationDate().getTime(), "Shoulda rawImported all contents.");
        assertEquals(0,  o21.getCreationDate().getTime(), "Shoulda rawImported all contents.");
        assertEquals(0,  o22.getCreationDate().getTime(), "Shoulda rawImported all contents.");

        final Object actual2 = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o20.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual2, "Shoulda rawImported all contents.");
        assertEquals("109", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o20.getId() ).getValue(), "Shoulda rawImported all contents.");
        final Object actual1 = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o21.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual1, "Shoulda rawImported all contents.");
        assertEquals("111", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o21.getId() ).getValue(), "Shoulda rawImported all contents.");
        final Object actual = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o22.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual, "Shoulda rawImported all contents.");
        assertEquals("114", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o22.getId() ).getValue(), "Shoulda rawImported all contents.");

        final List< Blob > blobs = new ArrayList<>( BeanUtils.sort(
                serviceManager.getRetriever( Blob.class ).retrieveAll( 
                        Blob.OBJECT_ID, o22.getId() ).toSet() ) );
        assertEquals(2,  blobs.size(), "Shoulda rawImported all contents.");
        assertEquals(0,  blobs.get(0).getByteOffset(), "Shoulda rawImported all contents.");
        assertEquals(1000,  blobs.get(0).getLength(), "Shoulda rawImported all contents.");
        assertEquals("115", blobs.get( 0 ).getChecksum(), "Shoulda rawImported all contents.");
        assertEquals(ChecksumType.MD5, blobs.get( 0 ).getChecksumType(), "Shoulda rawImported all contents.");
        assertEquals(1000,  blobs.get(1).getByteOffset(), "Shoulda rawImported all contents.");
        assertEquals(1000,  blobs.get(1).getLength(), "Shoulda rawImported all contents.");
        assertEquals("116", blobs.get( 1 ).getChecksum(), "Shoulda rawImported all contents.");
        assertEquals(ChecksumType.MD5, blobs.get( 1 ).getChecksumType(), "Shoulda rawImported all contents.");

        assertEquals(0,  serviceManager.getRetriever(RawImportTapeDirective.class).getCount(Require.nothing()), "Shoulda whacked rawImport directive from db.");
        assertEquals(numBlobTapesExpected,  serviceManager.getRetriever(BlobTape.class).getCount(Require.nothing()), "Shoulda recorded blobs on tape.");

        if ( verifyOrder )
        {
            verifyOrder( serviceManager );
        }
    }
    
    
    private void verifyOrder( final BeansServiceManager serviceManager )
    {
        final List< Blob > sortedBlobs = serviceManager.getRetriever( Blob.class ).retrieveAll().toList();
        Collections.sort( sortedBlobs, new BeanComparator<>( Blob.class, ChecksumObservable.CHECKSUM ) );
        
        final Map< UUID, Integer > orderIndexes = new HashMap<>();
        for ( final BlobTape bt : serviceManager.getRetriever( BlobTape.class ).retrieveAll().toSet() )
        {
            orderIndexes.put( bt.getBlobId(), Integer.valueOf( bt.getOrderIndex() ) );
        }
        
        int lastOrderIndex = -1;
        for ( final Blob blob : sortedBlobs )
        {
            final Integer orderIndex = orderIndexes.get( blob.getId() );
            if ( null == orderIndex )
            {
                continue;
            }
            if ( orderIndex.intValue() < lastOrderIndex )
            {
                fail( "Blobs rawImported out-of-order (order indexes should reflect order reported on tape)."
                        + "On blob " + blob.getId() + ", order index was " + orderIndex 
                        + ", but last order index was " + lastOrderIndex + "." );
            }
            lastOrderIndex = orderIndex.intValue();
        }
    }
    
    
    private List< S3ObjectsOnMedia > constructResponses( final int maxBucketsPerResponse )
    {
        return constructResponses( maxBucketsPerResponse, Integer.MAX_VALUE, true, 0, 1, false );
    }
    
    
    private List< S3ObjectsOnMedia > constructResponses(
            final int maxBucketsPerResponse, 
            final int zeroLengthBlobStep,
            final boolean includeTotalBlobCount,
            final int partNumber,
            final int numParts,
            final boolean interleaved )
    {
        assertFalse(interleaved && maxBucketsPerResponse > 1, "Interleaved construction is only implemented for single bucket responses");
        int checksum = 100;
        final List< S3ObjectOnMedia > ooms = new ArrayList<>();
        for ( int i = 0; i < 5; ++i )
        {
            for ( int j = 0; j <= i; ++j )
            {
                final List< S3ObjectMetadataKeyValue > metadatas = new ArrayList<>();
                ++checksum;
                metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                        .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                        .setValue( String.valueOf( ++checksum ) ) );
                
                final List< BlobOnMedia > bomsForObject = new ArrayList<>();
                final S3ObjectOnMedia oom = BeanFactory.newBean( S3ObjectOnMedia.class );
                final S3ObjectOnMedia oom2 = BeanFactory.newBean( S3ObjectOnMedia.class );
                oom.setId( UUID.randomUUID() );
                oom.setObjectName( "o_" + String.valueOf( i ) + "blobs_" + String.valueOf( j ) + "present" );
                int skippedBlobCount = 0;
                if ( ( 0 < checksum % zeroLengthBlobStep ) )
                {
                    for ( int k = i - j; k < i; ++k )
                    {
                        final BlobOnMedia bom = BeanFactory.newBean( BlobOnMedia.class );
                        bom.setChecksum( String.valueOf( ++checksum ) );
                        bom.setChecksumType( ChecksumType.MD5 );
                        bom.setId( UUID.randomUUID() );
                        bom.setLength( 1000 );
                        bom.setOffset( k * 1000 );
                        if ( partNumber == k % numParts )
                        {
                            bomsForObject.add( bom );
                        }
                        else
                        {
                            ++skippedBlobCount;
                        }
                    }
                }
                else
                {
                    final BlobOnMedia bom = BeanFactory.newBean( BlobOnMedia.class );
                    bom.setChecksum( String.valueOf( ++checksum ) );
                    bom.setChecksumType( ChecksumType.MD5 );
                    bom.setId( UUID.randomUUID() );
                    bom.setLength( 0 );
                    bom.setOffset( 0 );
                    bomsForObject.add( bom );
                }
                if ( includeTotalBlobCount )
                {
                    metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                            .setKey( KeyValueObservable.TOTAL_BLOB_COUNT )
                            .setValue( String.valueOf( bomsForObject.size() + skippedBlobCount ) ) );
                    metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                            .setKey( KeyValueObservable.CREATION_DATE )
                            .setValue( "0" ) );
                }
                oom.setMetadata( CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadatas ) );
                if ( interleaved )
                {
                    oom2.setMetadata(
                            CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadatas ) );
                    final List< BlobOnMedia > bomsForObject2 = new ArrayList<>();
                    for (int k = 0; k < bomsForObject.size() / 2; k++ )
                    {
                        bomsForObject2.add( bomsForObject.remove( bomsForObject.size() - 1 ) );
                    }
                    oom.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, bomsForObject ) );
                    oom2.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, bomsForObject2 ) );
                    if ( !bomsForObject2.isEmpty() )
                    {
                        oom2.setId( oom.getId() );
                        oom2.setObjectName( oom.getObjectName() );
                        ooms.add( oom2 );
                    }
                }
                else
                {
                    oom.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, bomsForObject ) );
                }
                ooms.add( oom );
            }
        }
        
        final List< S3ObjectOnMedia > oomsInBucket = new ArrayList<>();
        final List< BucketOnMedia > buckets = new ArrayList<>();
        if ( interleaved )
        {
            Collections.shuffle( ooms );
            final BucketOnMedia bucket = BeanFactory.newBean( BucketOnMedia.class );
            bucket.setBucketName( "b_0");
            bucket.setObjects( CollectionFactory.toArray( S3ObjectOnMedia.class, ooms ) );
            buckets.add( bucket );
        }
        else
        {
            for ( final S3ObjectOnMedia oom : ooms )
            {
                oomsInBucket.add( oom );
                if ( buckets.size() < oomsInBucket.size() )
                {
                    final BucketOnMedia bucket = BeanFactory.newBean( BucketOnMedia.class );
                    bucket.setBucketName( "b_" + oomsInBucket.size() );
                    bucket.setObjects( CollectionFactory.toArray( S3ObjectOnMedia.class, oomsInBucket ) );
                    buckets.add( bucket );
                    oomsInBucket.clear();
                }
            }
        }
            
        final List< BucketOnMedia > bucketsInResponse = new ArrayList<>();
        final List< S3ObjectsOnMedia > retval = new ArrayList<>();
        for ( final BucketOnMedia bucket : buckets )
        {
            bucketsInResponse.add( bucket );
            if ( bucketsInResponse.size() == maxBucketsPerResponse )
            {
                final S3ObjectsOnMedia response = BeanFactory.newBean( S3ObjectsOnMedia.class );
                response.setBuckets( CollectionFactory.toArray( BucketOnMedia.class, bucketsInResponse ) );
                retval.add( response );
            }
        }
        if ( !bucketsInResponse.isEmpty() )
        {
            final S3ObjectsOnMedia response = BeanFactory.newBean( S3ObjectsOnMedia.class );
            response.setBuckets( CollectionFactory.toArray( BucketOnMedia.class, bucketsInResponse ) );
            retval.add( response );
            bucketsInResponse.clear();
        }
        
        return retval;
    }
    
    
    private BlobStore getBlobStore()
    {
        return InterfaceProxyFactory.getProxy( BlobStore.class, null );
    }

    private RawImportTapeTask createTask( UUID tapeId, BeansServiceManager serviceManager) {
        return new RawImportTapeTask(
                BlobStoreTaskPriority.values()[ 0 ],
                tapeId,
                getBlobStore(),
                new MockDiskManager( serviceManager ),
                new TapeFailureManagement(serviceManager), serviceManager);
    }
}
