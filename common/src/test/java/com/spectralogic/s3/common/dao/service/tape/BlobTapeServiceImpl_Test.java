/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.HashSet;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class BlobTapeServiceImpl_Test 
{
    @Test
    public void testGetNextOrderIndexReturnsOneWhenNoBlobsExist()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final UUID tapeId = new MockDaoDriver( dbSupport ).createTape().getId();

        assertEquals(1,  dbSupport.getServiceManager().getService(BlobTapeService.class)
                .getNextOrderIndex(tapeId), "Shoulda returned one because there were no blobs on the tape.");
    }
    
    
    @Test
    public void testGetNextOrderIndexReturnsExpectedValueWhenTwoBlobsExist()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final String objectName = "test_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID tapeId = mockDaoDriver.createTapeBlobsFixture( objectName ).getId();

        assertEquals(1235,  dbSupport.getServiceManager().getService(BlobTapeService.class)
                .getNextOrderIndex(tapeId), "Shoulda returned the next greatest order index.");
    }
    
    
    @Test
    public void testDeleteObjectsOnTapeWhenBlobsAreOnTape()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID tapeId1 = mockDaoDriver.createTapeBlobsFixture( "text_object1" ).getId();
        final UUID tapeId2 = mockDaoDriver.createTapeBlobsFixture( "text_object2" ).getId();

        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        service.reclaimTape( "cause", tapeId1 );
        assertEquals(2,  service.getCount(), "Shoulda had exactly two blob tape links.");
        assertEquals(2,  service.getCount(Require.beanPropertyEqualsOneOf(BlobTape.TAPE_ID, tapeId2)), "Shoulda had exactly two blob tape links for the specified tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Tape.class).getCount(
                Require.beanPropertyEquals(PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, null)), "Shoulda whacked storage domain member assignment on the reclaimed tape only.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Tape.class).getCount(
                Require.beanPropertyEquals(
                        PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, Boolean.FALSE)), "Shoulda whacked bucket assignment on the reclaimed tape only.");
    }
    
    
    @Test
    public void testDeleteObjectsOnTapeWhenNoBlobsAreOnTape()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final UUID tapeId = new MockDaoDriver( dbSupport ).createTape().getId();

        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        service.reclaimTape( "cause", tapeId );
        assertEquals(0,  service.getCount(), "Should notta had any blob tape links.");
    }
    
    
    @Test
    public void testReclaimForDeletedPersistenceRuleDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final TapePartition tp1 =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        final Tape t4 = mockDaoDriver.createTape();
        final Tape t5 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy1.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy1.getId(), DataPersistenceRuleType.TEMPORARY, sd2.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );

        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        mockDaoDriver.updateBean(
                t1.setStorageDomainMemberId( sdm1.getId() ).setBucketId( bucket1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        mockDaoDriver.updateBean(
                t2.setStorageDomainMemberId( sdm1.getId() ).setBucketId( bucket2.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        mockDaoDriver.updateBean(
                t3.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean(
                t4.setStorageDomainMemberId( sdm2.getId() ).setBucketId( bucket1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        mockDaoDriver.updateBean(
                t5.setStorageDomainMemberId( sdm2.getId() ).setBucketId( bucket2.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final UUID id1 = mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() ).getId();
        final UUID id2 = mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() ).getId();
        final UUID id3 = mockDaoDriver.putBlobOnTape( t4.getId(), b2.getId() ).getId();
        final UUID id4 = mockDaoDriver.putBlobOnTape( t4.getId(), b3.getId() ).getId();
        
        final S3Object o4 = mockDaoDriver.createObject( bucket2.getId(), "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        final Blob b6 = mockDaoDriver.getBlobFor( o6.getId() );
        final UUID id5 = mockDaoDriver.putBlobOnTape( t1.getId(), b4.getId() ).getId();
        final UUID id6 = mockDaoDriver.putBlobOnTape( t1.getId(), b5.getId() ).getId();
        final UUID id7 = mockDaoDriver.putBlobOnTape( t4.getId(), b5.getId() ).getId();
        final UUID id8 = mockDaoDriver.putBlobOnTape( t4.getId(), b6.getId() ).getId();

        final Object expected2 = CollectionFactory.toSet( id1, id2, id3, id4, id5, id6, id7, id8 );
        assertEquals(expected2, BeanUtils.extractPropertyValues( 
                        dbSupport.getServiceManager().getRetriever( BlobTape.class ).retrieveAll().toSet(),
                        Identifiable.ID ), "Shoulda had all blob tape records initially.");

        dbSupport.getServiceManager().getService( BlobTapeService.class ).reclaimForDeletedPersistenceRule( 
                dataPolicy1.getId(), sd1.getId() );
        final Object expected1 = CollectionFactory.toSet( id3, id4, id5, id6, id7, id8 );
        assertEquals(expected1, BeanUtils.extractPropertyValues( 
                        dbSupport.getServiceManager().getRetriever( BlobTape.class ).retrieveAll().toSet(),
                        Identifiable.ID ), "Shoulda nuked correct blobs on tape.");
        assertNull(mockDaoDriver.attain( t1 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t2 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t4 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t5 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");

        dbSupport.getServiceManager().getService( BlobTapeService.class ).reclaimForDeletedPersistenceRule( 
                dataPolicy1.getId(), sd2.getId() );
        final Object expected = CollectionFactory.toSet( id5, id6, id7, id8 );
        assertEquals(expected, BeanUtils.extractPropertyValues( 
                        dbSupport.getServiceManager().getRetriever( BlobTape.class ).retrieveAll().toSet(),
                        Identifiable.ID ), "Shoulda nuked correct blobs on tape.");
        assertNull(mockDaoDriver.attain( t1 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t2 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNull(mockDaoDriver.attain( t4 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t5 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");

        assertNotNull(mockDaoDriver.attain( t1 ).getStorageDomainMemberId(), "Should notta nuked any storage domain member association.");
        assertNotNull(mockDaoDriver.attain( t2 ).getStorageDomainMemberId(), "Should notta nuked any storage domain member association.");
        assertNotNull(mockDaoDriver.attain( t3 ).getStorageDomainMemberId(), "Should notta nuked any storage domain member association.");
        assertNotNull(mockDaoDriver.attain( t4 ).getStorageDomainMemberId(), "Should notta nuked any storage domain member association.");
        assertNotNull(mockDaoDriver.attain( t5 ).getStorageDomainMemberId(), "Should notta nuked any storage domain member association.");
    }
    
    
    @Test
    public void testReclaimForTapeWhenTapeWasAssignedToStorageDomainAndNotBucketDoesntReclaimWhenSecureAlloc()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember storageDomainMember =
                mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.updateBean( 
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        mockDaoDriver.createBucket( null, "bucket" );
        
        final Tape tape = mockDaoDriver.createTape();
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( storageDomainMember.getId() )
                .setBucketId( null ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        dbSupport.getServiceManager().getService( BlobTapeService.class ).reclaimTape(
                "cause", tape.getId() );

        assertEquals(true, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).isAssignedToStorageDomain(), "Successful format should notta cleared assigned to bucket flag since securely allocated.");
        final Object expected = storageDomainMember.getId();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).getStorageDomainMemberId(), "Successful format should notta cleared cleared assigned to storage domain flag.");
    }
    
    
    @Test
    public void testReclaimForTapeWhenTapeWasNotAssignedToStorageDomainDoesReclaim()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.updateBean( 
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        
        final Tape tape = mockDaoDriver.createTape();
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( null )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        dbSupport.getServiceManager().getService( BlobTapeService.class ).reclaimTape(
                "cause", tape.getId() );

        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).isAssignedToStorageDomain(), "Successful format shoulda cleared assigned to bucket flag.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).getStorageDomainMemberId(), "Successful format shoulda cleared assignment to storage domain member.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).getBucketId(), "Successful format shoulda cleared assigned to bucket flag.");
    }
    
    
    @Test
    public void testReclaimForTapeWhenTapeWasAssignedToStorageDomainAndBucketNonSecurelyDoesReclaim()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember storageDomainMember = 
                mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        
        final Tape tape = mockDaoDriver.createTape();
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( storageDomainMember.getId() )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        dbSupport.getServiceManager().getService( BlobTapeService.class ).reclaimTape(
                "cause", tape.getId() );

        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).isAssignedToStorageDomain(), "Successful format shoulda cleared assigned to bucket flag.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).getStorageDomainMemberId(), "Successful format shoulda cleared assigment to storage domain.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).getBucketId(), "Successful format shoulda cleared assigned to bucket flag.");
    }
    
    
    @Test
    public void testReclaimForTapeWhenTapeWasAssignedToStorageDomainAndBucketSecurelyDoesNotReclaim()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember storageDomainMember =
                mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.updateBean( 
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        
        final Tape tape = mockDaoDriver.createTape();
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( storageDomainMember.getId() )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        dbSupport.getServiceManager().getService( BlobTapeService.class ).reclaimTape(
                "cause", tape.getId() );

        assertEquals(true, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).isAssignedToStorageDomain(), "Shoulda respected secure bucket isolation mode.");
        final Object expected1 = storageDomainMember.getId();
        assertEquals(expected1, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).getStorageDomainMemberId(), "Shoulda respected secure bucket isolation mode.");
        final Object expected = bucket.getId();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).getBucketId(), "Shoulda respected secure bucket isolation mode.");
    }

    
    @Test
    public void testBlobsLostEmptySetOfBlobsDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), null );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), null );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), null );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        service.blobsLost( null, p1.getId(), new HashSet< UUID >() );
        assertEquals(5,  service.getCount(), "Shoulda deleted nothing.");
    }
    
    
    @Test
    public void testBlobsLostDueToNormalOperationRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), null );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), null );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), null );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        service.blobsLost( null, p1.getId(), CollectionFactory.toSet( b1.getId() ) );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");

        service.blobsLost( null, p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on tape 1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded any degraded blobs.");
    }
    
    
    @Test
    public void testBlobsLostWhenSomeAlreadyRecordedRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), null );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), null );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), null );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        dbSupport.getDataManager().createBean( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b1.getId() )
                .setPersistenceRuleId( rule.getId() )
                .setBucketId( bucket1.getId() ) );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda recorded degraded blobs.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on tape 1.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda recorded degraded blobs.");
    }
    
    
    @Test
    public void testBlobsLostDueToErrorOutsideTransactionNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), null );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), null );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), null );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.blobsLost( "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            }
        } );
    }
    
    
    @Test
    public void testBlobsLostDueToErrorWhenPermanentPersistenceRuleRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), null );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), null );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), null );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda recorded degraded blobs.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on tape 1.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda recorded degraded blobs.");
    }
    
    
    @Test
    public void testBlobsLostDueToErrorWhenTemporaryPersistenceRuleRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), null );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, sd1.getId() );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), null );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), null );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since temp persistence rule.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on tape 1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since temp persistence rule.");
    }
    
    
    @Test
    public void testBlobsLostDueToErrorWhenRetiredPersistenceRuleRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), null );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd1.getId() );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), null );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), null );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since retired persistence rule.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on tape 1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since retired persistence rule.");
    }
    
    
    @Test
    public void testBlobsLostDueToErrorWhenNoPersistenceRuleRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), null );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), null );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), null );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since no persistence rule.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on tape 1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since no persistence rule.");
    }
    
    
    @Test
    public void testBlobsLostAcrossMultipleBucketsHandledCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), TapeType.LTO5 );
        
        final Tape p1 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL );
        final Tape p2 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                p1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobTapeService service = dbSupport.getServiceManager().getService( BlobTapeService.class );
        assertEquals(2,  service.getCount(), "Shoulda deleted all blobs lost.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsLost( 
                    "error", p2.getId(), CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(0,  service.getCount(), "Shoulda deleted all blobs lost.");
    }
    
    
    @Test
    public void testBlobsSuspectRecordsSuspectBlobs()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), partition1.getId(), TapeType.LTO5 );
        
        final Tape t1 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL );
        final Tape t2 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                t1.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                t2.setStorageDomainMemberId( sdm1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final BlobTape bp1 = mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        final BlobTape bp2 = mockDaoDriver.putBlobOnTape( t2.getId(), b1.getId() );
        final BlobTape bp3 = mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsSuspect( 
                    "error", CollectionFactory.toSet( bp1, bp2 ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final SuspectBlobTapeService service =
                dbSupport.getServiceManager().getService( SuspectBlobTapeService.class );
        assertEquals(2,  service.getCount(), "Shoulda recorded suspect blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda system failure for suspect blobs.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobTapeService.class ).blobsSuspect( 
                    "error", CollectionFactory.toSet( bp1, bp2, bp3 ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(3,  service.getCount(), "Shoulda recorded suspect blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda system failure for suspect blobs.");
    }
}
