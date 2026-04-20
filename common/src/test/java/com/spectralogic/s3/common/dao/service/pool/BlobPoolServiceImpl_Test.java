/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.Date;
import java.util.HashSet;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
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

public final class BlobPoolServiceImpl_Test 
{
    @Test
    public void testReclaimForDeletedPersistenceRuleDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final PoolPartition tp1 =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final Pool t1 = mockDaoDriver.createPool();
        final Pool t2 = mockDaoDriver.createPool();
        final Pool t3 = mockDaoDriver.createPool();
        final Pool t4 = mockDaoDriver.createPool();
        final Pool t5 = mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), tp1.getId() );
        final StorageDomainMember sdm2 = mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), tp1.getId() );
        
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
        final UUID id1 = mockDaoDriver.putBlobOnPool( t1.getId(), b1.getId() ).getId();
        final UUID id2 = mockDaoDriver.putBlobOnPool( t1.getId(), b2.getId() ).getId();
        final UUID id3 = mockDaoDriver.putBlobOnPool( t4.getId(), b2.getId() ).getId();
        final UUID id4 = mockDaoDriver.putBlobOnPool( t4.getId(), b3.getId() ).getId();
        
        final S3Object o4 = mockDaoDriver.createObject( bucket2.getId(), "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        final Blob b6 = mockDaoDriver.getBlobFor( o6.getId() );
        final UUID id5 = mockDaoDriver.putBlobOnPool( t1.getId(), b4.getId() ).getId();
        final UUID id6 = mockDaoDriver.putBlobOnPool( t1.getId(), b5.getId() ).getId();
        final UUID id7 = mockDaoDriver.putBlobOnPool( t4.getId(), b5.getId() ).getId();
        final UUID id8 = mockDaoDriver.putBlobOnPool( t4.getId(), b6.getId() ).getId();

        final Object expected2 = CollectionFactory.toSet( id1, id2, id3, id4, id5, id6, id7, id8 );
        assertEquals(expected2, BeanUtils.extractPropertyValues( 
                        dbSupport.getServiceManager().getRetriever( BlobPool.class ).retrieveAll().toSet(),
                        Identifiable.ID ), "Shoulda had all blob pool records initially.");

        dbSupport.getServiceManager().getService( BlobPoolService.class ).reclaimForDeletedPersistenceRule( 
                dataPolicy1.getId(), sd1.getId() );
        final Object expected1 = CollectionFactory.toSet( id3, id4, id5, id6, id7, id8 );
        assertEquals(expected1, BeanUtils.extractPropertyValues( 
                        dbSupport.getServiceManager().getRetriever( BlobPool.class ).retrieveAll().toSet(),
                        Identifiable.ID ), "Shoulda nuked correct blobs on pool.");
        assertNull(mockDaoDriver.attain( t1 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t2 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t4 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t5 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");

        dbSupport.getServiceManager().getService( BlobPoolService.class ).reclaimForDeletedPersistenceRule( 
                dataPolicy1.getId(), sd2.getId() );
        final Object expected = CollectionFactory.toSet( id5, id6, id7, id8 );
        assertEquals(expected, BeanUtils.extractPropertyValues( 
                        dbSupport.getServiceManager().getRetriever( BlobPool.class ).retrieveAll().toSet(),
                        Identifiable.ID ), "Shoulda nuked correct blobs on pool.");
        assertNull(mockDaoDriver.attain( t1 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t2 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNull(mockDaoDriver.attain( t4 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");
        assertNotNull(mockDaoDriver.attain( t5 ).getBucketId(), "Shoulda nuked bucket association on media as necessary.");

        assertNotNull(mockDaoDriver.attain( t1 ).getStorageDomainMemberId(), "Should notta nuked any storage domain association.");
        assertNotNull(mockDaoDriver.attain( t2 ).getStorageDomainMemberId(), "Should notta nuked any storage domain association.");
        assertNotNull(mockDaoDriver.attain( t3 ).getStorageDomainMemberId(), "Should notta nuked any storage domain association.");
        assertNotNull(mockDaoDriver.attain( t4 ).getStorageDomainMemberId(), "Should notta nuked any storage domain association.");
        assertNotNull(mockDaoDriver.attain( t5 ).getStorageDomainMemberId(), "Should notta nuked any storage domain association.");
    }
    
    
    @Test
    public void testReclaimForTemporaryPersistenceRuleNullPoolNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final Pool pool = mockDaoDriver.createPool();
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() );
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.reclaimForTemporaryPersistenceRule(
                        null,
                        bucket.getId(), 
                        1,
                        BlobPool.DATE_WRITTEN );
            }
        } );
    }
    
    
    @Test
    public void testReclaimForTemporaryPersistenceRuleNullBucketNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final Pool pool = mockDaoDriver.createPool();
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() );
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.reclaimForTemporaryPersistenceRule(
                        pool.getId(),
                        null,
                        1,
                        BlobPool.DATE_WRITTEN );
            }
        } );
    }
    
    
    @Test
    public void testReclaimForTemporaryPersistenceRuleNullBeanPropertyNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final Pool pool = mockDaoDriver.createPool();
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() );
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.reclaimForTemporaryPersistenceRule(
                        pool.getId(),
                        bucket.getId(), 
                        1,
                        null );
            }
        } );
    }
    
    
    @Test
    public void testReclaimForTemporaryPersistenceRuleDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket3" );
        final StorageDomain storageDomain1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain storageDomain2 = mockDaoDriver.createStorageDomain( "sd2" );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final Pool pool3 = mockDaoDriver.createPool();
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain1.getId(), partition1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain2.getId(), partition2.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy1.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy2.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        
        final long dayMultiplier = 3600L * 1000 * 24;
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 50 * dayMultiplier ) )
                .setLastAccessed( new Date( System.currentTimeMillis() - 10 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN, BlobPool.LAST_ACCESSED );
        
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.putBlobOnPool( pool1.getId(), b2.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 5 * dayMultiplier ) )
                .setLastAccessed( new Date( System.currentTimeMillis() - 1 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN, BlobPool.LAST_ACCESSED );
        
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.putBlobOnPool( pool2.getId(), b3.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 5 * dayMultiplier ) )
                .setLastAccessed( new Date( System.currentTimeMillis() - 1 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN, BlobPool.LAST_ACCESSED );
        
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.putBlobOnPool( pool2.getId(), b4.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 2 * dayMultiplier ) )
                .setLastAccessed( new Date( System.currentTimeMillis() - 4 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN, BlobPool.LAST_ACCESSED );
        
        final S3Object o21 = mockDaoDriver.createObject( bucket2.getId(), "o1" );
        final Blob b21 = mockDaoDriver.getBlobFor( o21.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.putBlobOnPool( pool1.getId(), b21.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 50 * dayMultiplier ) )
                .setLastAccessed( new Date( System.currentTimeMillis() - 10 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN, BlobPool.LAST_ACCESSED );
        
        final S3Object o22 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b22 = mockDaoDriver.getBlobFor( o22.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.putBlobOnPool( pool1.getId(), b22.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 5 * dayMultiplier ) )
                .setLastAccessed( new Date( System.currentTimeMillis() - 1 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN, BlobPool.LAST_ACCESSED );
        
        final S3Object o23 = mockDaoDriver.createObject( bucket2.getId(), "o3" );
        final Blob b23 = mockDaoDriver.getBlobFor( o23.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.putBlobOnPool( pool2.getId(), b23.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 5 * dayMultiplier ) )
                .setLastAccessed( new Date( System.currentTimeMillis() - 1 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN, BlobPool.LAST_ACCESSED );
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        assertEquals(7,  service.getCount(), "Should notta deleted anything yet.");

        service.reclaimForTemporaryPersistenceRule(
                pool1.getId(),
                bucket3.getId(), 
                1,
                BlobPool.DATE_WRITTEN );
        assertEquals(7,  service.getCount(), "Should notta deleted anything yet.");

        service.reclaimForTemporaryPersistenceRule(
                pool1.getId(),
                bucket1.getId(), 
                51,
                BlobPool.DATE_WRITTEN );
        assertEquals(7,  service.getCount(), "Should notta deleted anything yet.");

        service.reclaimForTemporaryPersistenceRule(
                pool3.getId(),
                bucket1.getId(), 
                0,
                BlobPool.DATE_WRITTEN );
        assertEquals(7,  service.getCount(), "Should notta deleted anything yet.");

        service.reclaimForTemporaryPersistenceRule(
                pool1.getId(),
                bucket1.getId(), 
                49,
                BlobPool.DATE_WRITTEN );
        assertEquals(6,  service.getCount(), "Shoulda deleted as necessary.");

        service.reclaimForTemporaryPersistenceRule(
                pool1.getId(),
                bucket1.getId(), 
                49,
                BlobPool.DATE_WRITTEN );
        assertEquals(6,  service.getCount(), "Should notta deleted anything.");

        service.reclaimForTemporaryPersistenceRule(
                pool1.getId(),
                bucket1.getId(), 
                3,
                BlobPool.LAST_ACCESSED );
        assertEquals(6,  service.getCount(), "Should notta deleted anything.");

        service.reclaimForTemporaryPersistenceRule(
                pool2.getId(),
                bucket1.getId(), 
                3,
                BlobPool.LAST_ACCESSED );
        assertEquals(5,  service.getCount(), "Shoulda deleted as necessary.");

        service.reclaimForTemporaryPersistenceRule(
                pool1.getId(),
                bucket1.getId(),
                1,
                BlobPool.LAST_ACCESSED );
        assertEquals(4,  service.getCount(), "Shoulda deleted as necessary.");
    }


    @Test
    public void testReclaimForTemporaryPersistenceRuleDoesNotReclaimBlobsWithJobEntries()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final Pool pool = mockDaoDriver.createPool();
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() );

        final long dayMultiplier = 3600L * 1000 * 24;

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean(
                mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 50 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.updateBean(
                mockDaoDriver.putBlobOnPool( pool.getId(), b2.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 50 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.updateBean(
                mockDaoDriver.putBlobOnPool( pool.getId(), b3.getId() )
                .setDateWritten( new Date( System.currentTimeMillis() - 50 * dayMultiplier ) ),
                BlobPool.DATE_WRITTEN );

        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createJobEntry( job.getId(), b3 );

        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        assertEquals(3,  service.getCount(), "Should notta deleted anything yet.");

        service.reclaimForTemporaryPersistenceRule(
                pool.getId(),
                bucket.getId(),
                49,
                BlobPool.DATE_WRITTEN );
        assertEquals(2,  service.getCount(), "Shoulda deleted only blob without job entry.");

        assertNotNull(dbSupport.getServiceManager().getRetriever(BlobPool.class).retrieve(
                Require.beanPropertyEquals(BlobObservable.BLOB_ID, b1.getId())), "Shoulda kept blob with job entry.");
        assertNull(dbSupport.getServiceManager().getRetriever(BlobPool.class).retrieve(
                Require.beanPropertyEquals(BlobObservable.BLOB_ID, b2.getId())), "Shoulda deleted blob without job entry.");
        assertNotNull(dbSupport.getServiceManager().getRetriever(BlobPool.class).retrieve(
                Require.beanPropertyEquals(BlobObservable.BLOB_ID, b3.getId())), "Shoulda kept blob with job entry.");
    }


    @Test
    public void testUpdateLastAccessedNullBlobIdsNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                dbSupport.getServiceManager().getService( BlobPoolService.class ).updateLastAccessed( null );
            }
        } );
    }
    
    
    @Test
    public void testUpdateLastAccessedDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final StorageDomain storageDomain1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain storageDomain2 = mockDaoDriver.createStorageDomain( "sd2" );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain1.getId(), partition1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain2.getId(), partition2.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy1.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy2.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b2.getId() );
        
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b3.getId() );
        
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b4.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b4.getId() );
        
        final S3Object o21 = mockDaoDriver.createObject( bucket2.getId(), "o1" );
        final Blob b21 = mockDaoDriver.getBlobFor( o21.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b21.getId() );
        
        final S3Object o22 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b22 = mockDaoDriver.getBlobFor( o22.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b22.getId() );
        
        final S3Object o23 = mockDaoDriver.createObject( bucket2.getId(), "o3" );
        final Blob b23 = mockDaoDriver.getBlobFor( o23.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b23.getId() );
        
        TestUtil.sleep( 10 );
        final Date date = new Date();
        TestUtil.sleep( 10 );
        
        dbSupport.getServiceManager().getService( BlobPoolService.class ).updateLastAccessed( 
                CollectionFactory.toSet( b3.getId() ) );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(
                Require.beanPropertyGreaterThan(BlobPool.LAST_ACCESSED, date)), "Shoulda updated only entries that were for b3.");
    }
    
    
    @Test
    public void testBlobsLostEmptySetOfBlobsDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
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
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        service.blobsLost( null, p1.getId(), CollectionFactory.toSet( b1.getId() ) );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");

        service.blobsLost( null, p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on pool 1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded any degraded blobs.");
    }
    
    
    @Test
    public void testBlobsLostDueToErrorOutsideTransactionNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
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
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda recorded degraded blobs.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on pool 1.");
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
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, sd1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since temp persistence rule.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on pool 1.");
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
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since retired persistence rule.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on pool 1.");
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
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since no persistence rule.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on pool 1.");
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
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
                    "error", p1.getId(), CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        assertEquals(2,  service.getCount(), "Shoulda deleted all blobs lost.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsLost( 
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
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "pp1" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), partition1.getId() );
        
        final Pool p1 = mockDaoDriver.createPool( partition1.getId(), null );
        final Pool p2 = mockDaoDriver.createPool( partition1.getId(), null );
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
        final BlobPool bp1 = mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        final BlobPool bp2 = mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        final BlobPool bp3 = mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsSuspect( 
                    "error", CollectionFactory.toSet( bp1, bp2 ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final SuspectBlobPoolService service =
                dbSupport.getServiceManager().getService( SuspectBlobPoolService.class );
        assertEquals(2,  service.getCount(), "Shoulda recorded suspect blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda system failure for suspect blobs.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsSuspect( 
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
