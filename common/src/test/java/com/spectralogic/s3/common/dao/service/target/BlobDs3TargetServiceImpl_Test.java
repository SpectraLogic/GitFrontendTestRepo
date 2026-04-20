/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.HashSet;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class BlobDs3TargetServiceImpl_Test 
{
    @Test
    public void testMigrateDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final TapePartition tp1 =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm1 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm2 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( sdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( sdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( sdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( sdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b3.getId() );
        
        final BlobDs3Target bt1 = BeanFactory.newBean( BlobDs3Target.class );
        bt1.setBlobId( b1.getId() );
        bt1.setTargetId( target.getId() );

        final BlobDs3Target bt2 = BeanFactory.newBean( BlobDs3Target.class );
        bt2.setBlobId( b2.getId() );
        bt2.setTargetId( target.getId() );

        final BlobDs3Target bt3 = BeanFactory.newBean( BlobDs3Target.class );
        bt3.setBlobId( b3.getId() );
        bt3.setTargetId( target.getId() );
        
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).migrate( 
                    sd1.getId(),
                    CollectionFactory.toSet( bt1, bt2, bt3 ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda created blob targets.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Shoulda whacked blob tapes.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Shoulda whacked blob pools.");
    }
    
    
    @Test
    public void testReclaimForDeletedDs3TargetDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        final Ds3DataReplicationRule rule = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), 
                DataReplicationRuleType.PERMANENT,
                target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), 
                DataReplicationRuleType.PERMANENT,
                target2.getId() );
        
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "b1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        
        final S3Object otherObject = mockDaoDriver.createObject( null, "o1" );
        final Blob otherBlob = mockDaoDriver.getBlobFor( otherObject.getId() );
        
        final BlobDs3Target br = mockDaoDriver.putBlobOnDs3Target( target1.getId(), blob.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), blob.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), otherBlob.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), otherBlob.getId() );
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Should notta cleared blobs on target1 for buckets using dp yet.");

        dbSupport.getServiceManager().getService( BlobDs3TargetService.class )
            .reclaimForDeletedReplicationRule( dp.getId(), rule );

        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Successful cleared blobs on target1 for buckets using dp.");
        assertNull(
                mockDaoDriver.retrieve( br ),
                "Successful cleared blobs on target1 for buckets using dp."
                 );
    }
    
    
    @Test
    public void testReclaimForDeletedReplicationRuleWithoutTargetNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        final Ds3DataReplicationRule rule = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), 
                DataReplicationRuleType.PERMANENT,
                target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), 
                DataReplicationRuleType.PERMANENT,
                target2.getId() );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class )
                    .reclaimForDeletedReplicationRule( dp.getId(), rule.setTargetId( null ) );
            }
        } );
    }

    
    @Test
    public void testBlobsLostEmptySetOfBlobsDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        
        final BlobDs3TargetService service = 
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class );
        service.blobsLost( null, target1.getId(), new HashSet< UUID >() );
        assertEquals(5,  service.getCount(), "Shoulda deleted nothing.");
    }
    
    
    @Test
    public void testBlobsSuspectDelegatesToBlobsLost()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        
        final BlobDs3TargetService service =
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class );
        service.blobsSuspect( 
                null, 
                CollectionFactory.toSet( 
                        BeanFactory.newBean( BlobDs3Target.class )
                        .setBlobId( b1.getId() ).setTargetId( target1.getId() ) ) );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");

        service.blobsSuspect( 
                null, 
                CollectionFactory.toSet( 
                        BeanFactory.newBean( BlobDs3Target.class )
                        .setBlobId( b2.getId() ).setTargetId( target1.getId() ),
                        BeanFactory.newBean( BlobDs3Target.class )
                        .setBlobId( b3.getId() ).setTargetId( target1.getId() ) ) );
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on target 1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded any degraded blobs.");
    }
    
    
    @Test
    public void testBlobsLostDueToNormalOperationRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        
        final BlobDs3TargetService service =
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class );
        service.blobsLost( 
                null, 
                target1.getId(), 
                CollectionFactory.toSet( b1.getId() ) );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");

        service.blobsLost(
                null,
                target1.getId(),
                CollectionFactory.toSet( b2.getId(), b3.getId() ) );
        assertEquals(2,  service.getCount(), "Shoulda deleted the blobs on target 1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded any degraded blobs.");
    }
    
    
    @Test
    public void testBlobsLostDueToErrorOutsideTransactionNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        
        final BlobDs3TargetService service =
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.blobsLost(
                        "error", 
                        target1.getId(), 
                        CollectionFactory.toSet( b1.getId() ) );
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
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).blobsLost( 
                    "error",
                    target1.getId(),
                    CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobDs3TargetService service = 
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda recorded degraded blobs.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).blobsLost( 
                    "error", 
                    target1.getId(),
                    CollectionFactory.toSet( b2.getId(), b3.getId() ) );
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
    public void testBlobsLostDueToErrorWhenRetiredPersistenceRuleRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.RETIRED, target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).blobsLost( 
                    "error",
                    target1.getId(), 
                    CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobDs3TargetService service =
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since temp persistence rule.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).blobsLost( 
                    "error", 
                    target1.getId(), 
                    CollectionFactory.toSet( b2.getId(), b3.getId() ) );
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
    public void testBlobsLostDueToErrorWhenNoPersistenceRuleRecordsLoss()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).blobsLost( 
                    "error", 
                    target1.getId(),
                    CollectionFactory.toSet( b1.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobDs3TargetService service = 
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class );
        assertEquals(4,  service.getCount(), "Shoulda deleted the single blob only.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta recorded degraded blobs since no persistence rule.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).blobsLost( 
                    "error", 
                    target1.getId(),
                    CollectionFactory.toSet( b2.getId(), b3.getId() ) );
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
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        
        BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).blobsLost( 
                    "error", 
                    target1.getId(), 
                    CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final BlobDs3TargetService service =
                dbSupport.getServiceManager().getService( BlobDs3TargetService.class );
        assertEquals(2,  service.getCount(), "Shoulda deleted all blobs lost.");

        transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobDs3TargetService.class ).blobsLost( 
                    "error", 
                    target2.getId(), 
                    CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(0,  service.getCount(), "Shoulda deleted all blobs lost.");
    }
}
