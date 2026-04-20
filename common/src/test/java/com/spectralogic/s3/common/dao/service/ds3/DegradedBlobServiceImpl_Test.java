/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class DegradedBlobServiceImpl_Test 
{

    @Test
    public void testCreateCreatesBlobDegradationNotAlreadyRecorded()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule( 
                dp1.getId(), DataPersistenceRuleType.values()[ 0 ], sd1.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.values()[ 0 ], sd2.getId() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dp1.getId(), "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp1.getId(), "b2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dp2.getId(), "b3" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket3.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket3.getId(), "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b2.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b3.getId(), rule2.getId() );
        
        final Set< DegradedBlob > degradedBlobs = new HashSet<>();
        degradedBlobs.add( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b1.getId() ).setBucketId( bucket1.getId() )
                .setPersistenceRuleId( rule1.getId() ) );
        degradedBlobs.add( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b1.getId() ).setBucketId( bucket1.getId() )
                .setPersistenceRuleId( rule2.getId() ) );
        degradedBlobs.add( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b2.getId() ).setBucketId( bucket2.getId() )
                .setPersistenceRuleId( rule1.getId() ) );
        degradedBlobs.add( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b2.getId() ).setBucketId( bucket2.getId() )
                .setPersistenceRuleId( rule2.getId() ) );
        degradedBlobs.add( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b3.getId() ).setBucketId( bucket3.getId() )
                .setPersistenceRuleId( rule1.getId() ) );
        degradedBlobs.add( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b3.getId() ).setBucketId( bucket3.getId() )
                .setPersistenceRuleId( rule2.getId() ) );
        degradedBlobs.add( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b4.getId() ).setBucketId( bucket3.getId() )
                .setPersistenceRuleId( rule1.getId() ) );
        degradedBlobs.add( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b4.getId() ).setBucketId( bucket3.getId() )
                .setPersistenceRuleId( rule2.getId() ) );
        
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( DegradedBlobService.class ).create( degradedBlobs );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        assertEquals(8,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda created degraded blobs that didn't already exist.");
    }
    
    
    @Test
    public void testMigrateDoesSoForDataPersistenceRules()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule( 
                dp1.getId(), DataPersistenceRuleType.values()[ 0 ], sd1.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.values()[ 0 ], sd2.getId() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dp1.getId(), "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp1.getId(), "b2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dp2.getId(), "b3" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket3.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b2.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b3.getId(), rule2.getId() );
        
        final DegradedBlobService service =
                dbSupport.getServiceManager().getService( DegradedBlobService.class );
        service.migrate( DegradedBlob.PERSISTENCE_RULE_ID, bucket1.getId(), rule2.getId(), rule1.getId() );

        assertEquals(1,  service.getCount(Require.beanPropertyEquals(
                DegradedBlob.PERSISTENCE_RULE_ID, rule1.getId())), "Shoulda migrated degraded blobs.");
        assertEquals(2,  service.getCount(Require.beanPropertyEquals(
                DegradedBlob.PERSISTENCE_RULE_ID, rule2.getId())), "Shoulda migrated degraded blobs.");
    }
    
    
    @Test
    public void testMigrateDoesSoForDataReplicationRules()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final Ds3Target sd1 = mockDaoDriver.createDs3Target( "sd1" );
        final DataReplicationRule< ? > rule1 = mockDaoDriver.createDs3DataReplicationRule(  
                dp1.getId(), DataReplicationRuleType.values()[ 0 ], sd1.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        final DataReplicationRule< ? > rule2 = mockDaoDriver.createDs3DataReplicationRule( 
                dp2.getId(), DataReplicationRuleType.values()[ 0 ], sd2.getId() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dp1.getId(), "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp1.getId(), "b2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dp2.getId(), "b3" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket3.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b2.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b3.getId(), rule2.getId() );
        
        final DegradedBlobService service =
                dbSupport.getServiceManager().getService( DegradedBlobService.class );
        service.migrate( 
                DegradedBlob.DS3_REPLICATION_RULE_ID, bucket1.getId(), rule2.getId(), rule1.getId() );

        assertEquals(1,  service.getCount(Require.beanPropertyEquals(
                DegradedBlob.DS3_REPLICATION_RULE_ID, rule1.getId())), "Shoulda migrated degraded blobs.");
        assertEquals(2,  service.getCount(Require.beanPropertyEquals(
                DegradedBlob.DS3_REPLICATION_RULE_ID, rule2.getId())), "Shoulda migrated degraded blobs.");
    }
    
    
    @Test
    public void testDeleteAllForPersistenceRuleDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule( 
                dp1.getId(), DataPersistenceRuleType.values()[ 0 ], sd1.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.values()[ 0 ], sd2.getId() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dp1.getId(), "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp1.getId(), "b2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dp2.getId(), "b3" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket3.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b2.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b3.getId(), rule2.getId() );
        
        final DegradedBlobService service =
                dbSupport.getServiceManager().getService( DegradedBlobService.class );
        service.deleteAllForPersistenceRule( rule1.getId() );
        assertEquals(1,  service.getCount(), "Shoulda deleted all degraded blobs for rule1.");
    }
    
    
    @Test
    public void testDeleteForPersistenceAndReplicationRulesDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule( 
                dp1.getId(), DataPersistenceRuleType.values()[ 0 ], sd1.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.values()[ 0 ], sd2.getId() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dp1.getId(), "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp1.getId(), "b2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dp2.getId(), "b3" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket3.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );

        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "t3" );
        final S3Target target4 = mockDaoDriver.createS3Target( "t4" );
        final DataReplicationRule< ? > rule3 = mockDaoDriver.createDs3DataReplicationRule(
                dp1.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        final DataReplicationRule< ? > rule4 = mockDaoDriver.createDs3DataReplicationRule(
                dp1.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        final DataReplicationRule< ? > rule5 = mockDaoDriver.createAzureDataReplicationRule( 
                dp1.getId(), DataReplicationRuleType.PERMANENT, target3.getId() );
        final DataReplicationRule< ? > rule6 = mockDaoDriver.createS3DataReplicationRule( 
                dp1.getId(), DataReplicationRuleType.PERMANENT, target4.getId() );
        
        mockDaoDriver.createDegradedBlob( b1.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule2.getId() );
        mockDaoDriver.createDegradedBlob( b2.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b3.getId(), rule2.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule3.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule4.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule5.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule6.getId() );
        
        final Pool pool1 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool1.setLastVerified( new Date() ), PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() ) );
        
        final Pool pool2 = mockDaoDriver.createPool();
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() ) );
        
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnAzureTarget( target3.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnS3Target( target4.getId(), b1.getId() ) );
        
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( tape1.setLastVerified( new Date() ), PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape1.getId(), b1.getId() ) );
        
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() ) );
        
        final Tape tape3 = mockDaoDriver.createTape();
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape3.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape3.getId(), b2.getId() ) );
        
        final DegradedBlobService service =
                dbSupport.getServiceManager().getService( DegradedBlobService.class );
        service.deleteForPersistenceRule( 
                rule1.getId(),
                BlobTape.class,
                BlobTape.TAPE_ID,
                tape1.getId(),
                CollectionFactory.toSet( b1.getId() ) );
        assertEquals(7,  service.getCount(), "Shoulda deleted specified degraded blobs for rule1.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda deleted specified suspect blobs for tape.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda deleted specified suspect blobs for pool.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(SuspectBlobDs3Target.class).getCount(), "Shoulda deleted specified suspect blobs for DS3 target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Shoulda deleted specified suspect blobs for Azure target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobS3Target.class).getCount(), "Shoulda deleted specified suspect blobs for S3 target.");

        service.deleteForPersistenceRule(
                rule1.getId(),
                BlobPool.class,
                BlobPool.POOL_ID,
                pool1.getId(),
                CollectionFactory.toSet( b1.getId() ) );
        assertEquals(7,  service.getCount(), "Shoulda deleted specified degraded blobs for rule1.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda deleted specified suspect blobs for tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda deleted specified suspect blobs for pool.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(SuspectBlobDs3Target.class).getCount(), "Shoulda deleted specified suspect blobs for target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Shoulda deleted specified suspect blobs for Azure target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobS3Target.class).getCount(), "Shoulda deleted specified suspect blobs for S3 target.");

        service.deleteForReplicationRule(
                DegradedBlob.DS3_REPLICATION_RULE_ID,
                rule3.getId(),
                BlobDs3Target.class,
                target1.getId(),
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        assertEquals(6,  service.getCount(), "Shoulda deleted specified degraded blobs for rule1.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda deleted specified suspect blobs for tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda deleted specified suspect blobs for pool.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobDs3Target.class).getCount(), "Shoulda deleted specified suspect blobs for target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Shoulda deleted specified suspect blobs for Azure target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobS3Target.class).getCount(), "Shoulda deleted specified suspect blobs for S3 target.");

        service.deleteForPersistenceRule( 
                rule2.getId(),
                BlobTape.class,
                BlobTape.TAPE_ID,
                tape3.getId(),
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        assertEquals(4,  service.getCount(), "Shoulda deleted specified degraded blobs for rule1.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda deleted specified suspect blobs for tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda deleted specified suspect blobs for pool.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobDs3Target.class).getCount(), "Shoulda deleted specified suspect blobs for target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Shoulda deleted specified suspect blobs for Azure target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobS3Target.class).getCount(), "Shoulda deleted specified suspect blobs for S3 target.");

        service.deleteForPersistenceRule( 
                rule2.getId(),
                BlobTape.class,
                BlobTape.TAPE_ID,
                tape2.getId(),
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        assertEquals(4,  service.getCount(), "Shoulda deleted specified degraded blobs for rule1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda deleted specified suspect blobs for tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda deleted specified suspect blobs for pool.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobDs3Target.class).getCount(), "Shoulda deleted specified suspect blobs for target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Shoulda deleted specified suspect blobs for Azure target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobS3Target.class).getCount(), "Shoulda deleted specified suspect blobs for S3 target.");

        service.deleteForReplicationRule(
                DegradedBlob.AZURE_REPLICATION_RULE_ID,
                rule5.getId(),
                BlobAzureTarget.class,
                target3.getId(),
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        assertEquals(3,  service.getCount(), "Shoulda deleted specified degraded blobs for rule1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda deleted specified suspect blobs for tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda deleted specified suspect blobs for pool.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobDs3Target.class).getCount(), "Shoulda deleted specified suspect blobs for target.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Shoulda deleted specified suspect blobs for Azure target.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobS3Target.class).getCount(), "Shoulda deleted specified suspect blobs for S3 target.");

        service.deleteForReplicationRule(
                DegradedBlob.S3_REPLICATION_RULE_ID,
                rule6.getId(),
                BlobS3Target.class,
                target4.getId(),
                CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        assertEquals(2,  service.getCount(), "Shoulda deleted specified degraded blobs for rule1.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(), "Shoulda deleted specified suspect blobs for tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda deleted specified suspect blobs for pool.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobDs3Target.class).getCount(), "Shoulda deleted specified suspect blobs for target.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Shoulda deleted specified suspect blobs for Azure target.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(SuspectBlobS3Target.class).getCount(), "Shoulda deleted specified suspect blobs for S3 target.");

        assertNotNull(mockDaoDriver.attain( tape1 ).getLastVerified(), "Should notta reset last verified attributes for operation.");
        assertNotNull(mockDaoDriver.attain( pool1 ).getLastVerified(), "Should notta reset last verified attributes for operation.");
    }
    
    
    @Test
    public void testDeleteAllForReplicationRuleDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        final Ds3Target sd1 = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3DataReplicationRule rule1 = mockDaoDriver.createDs3DataReplicationRule( 
                dp1.getId(), DataReplicationRuleType.values()[ 0 ], sd1.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        final Ds3DataReplicationRule rule2 = mockDaoDriver.createDs3DataReplicationRule( 
                dp2.getId(), DataReplicationRuleType.values()[ 0 ], sd2.getId() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dp1.getId(), "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp1.getId(), "b2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dp2.getId(), "b3" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket3.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.createDegradedBlob( b1.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b2.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( b3.getId(), rule2.getId() );
        
        final DegradedBlobService service =
                dbSupport.getServiceManager().getService( DegradedBlobService.class );
        service.deleteAllForReplicationRule( DegradedBlob.DS3_REPLICATION_RULE_ID, rule1.getId() );
        assertEquals(1,  service.getCount(), "Shoulda deleted all degraded blobs for rule1.");
    }
}
