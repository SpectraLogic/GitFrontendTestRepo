/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.frontend;

import java.util.Collections;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacementRuleState;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.PublicCloudDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3Region;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMemberState;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public final class DataPolicyManagementResourceImpl_Test
{
     @Test
    public void testConstructorNullRpcServerNotAllowed()
    {
        
        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            @Override
        public void test() throws Throwable
            {
                new DataPolicyManagementResourceImpl(
                        null,
                        dbSupport.getServiceManager() );
            }
        } );
    }
    

     @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Override

        public void test() throws Throwable
            {
                new DataPolicyManagementResourceImpl(
                        InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                        null );
            }
        } );
    }
    

     @Test
    public void testHappyConstruction()
    {
        
        new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
    }
    
    
     @Test
    public void testConstructionResultsInStandardIsolatedBucketsWithDedicatedPersistenceTargetsAreFixed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp1 =
                mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = 
                mockDaoDriver.createDataPolicy( "policy2" );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dp1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dp2.getId(), "bucket2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD,
                dp1.getId(),
                DataPersistenceRuleType.PERMANENT, 
                sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD,
                dp1.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd2.getId() );
        
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED,
                dp2.getId(), 
                DataPersistenceRuleType.PERMANENT, 
                sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED,
                dp2.getId(), 
                DataPersistenceRuleType.TEMPORARY, 
                sd2.getId() );
        
        final Tape tape1 = mockDaoDriver.createTape( null, TapeState.NORMAL );
        final StorageDomainMember tsdm1 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), tape1.getPartitionId(), tape1.getType() );
        final StorageDomainMember tsdm2 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( tsdm1.getId() ).setBucketId( bucket1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape2 = mockDaoDriver.createTape( null, TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( tsdm2.getId() ).setBucketId( bucket1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape3 = mockDaoDriver.createTape( null, TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                tape3.setStorageDomainMemberId( tsdm1.getId() ).setBucketId( bucket2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape4 = mockDaoDriver.createTape( null, TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                tape4.setStorageDomainMemberId( tsdm2.getId() ).setBucketId( bucket2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape5 = mockDaoDriver.createTape( null, TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                tape5.setStorageDomainMemberId( tsdm1.getId() ).setBucketId( null ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape6 = mockDaoDriver.createTape( null, TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                tape6.setStorageDomainMemberId( tsdm2.getId() ).setBucketId( null ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape7 = mockDaoDriver.createTape( null, TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                tape7.setStorageDomainMemberId( null ).setBucketId( null ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        
        final Pool pool1 = mockDaoDriver.createPool();
        final StorageDomainMember psdm1 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), pool1.getPartitionId() );
        final StorageDomainMember psdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), pool1.getPartitionId() );
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( psdm1.getId() ).setBucketId( bucket1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Pool pool2 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( psdm2.getId() ).setBucketId( bucket1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Pool pool3 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                pool3.setStorageDomainMemberId( psdm1.getId() ).setBucketId( bucket2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Pool pool4 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                pool4.setStorageDomainMemberId( psdm2.getId() ).setBucketId( bucket2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Pool pool5 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                pool5.setStorageDomainMemberId( psdm1.getId() ).setBucketId( null ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Pool pool6 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                pool6.setStorageDomainMemberId( psdm2.getId() ).setBucketId( null ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Pool pool7 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                pool7.setStorageDomainMemberId( null ).setBucketId( null ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        
        new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        mockDaoDriver.attainAndUpdate( tape1 );
        mockDaoDriver.attainAndUpdate( tape2 );
        mockDaoDriver.attainAndUpdate( tape3 );
        mockDaoDriver.attainAndUpdate( tape4 );
        mockDaoDriver.attainAndUpdate( tape5 );
        mockDaoDriver.attainAndUpdate( tape6 );
        mockDaoDriver.attainAndUpdate( tape7 );
        mockDaoDriver.attainAndUpdate( pool1 );
        mockDaoDriver.attainAndUpdate( pool2 );
        mockDaoDriver.attainAndUpdate( pool3 );
        mockDaoDriver.attainAndUpdate( pool4 );
        mockDaoDriver.attainAndUpdate( pool5 );
        mockDaoDriver.attainAndUpdate( pool6 );
        mockDaoDriver.attainAndUpdate( pool7 );

        final Object expected15 = tsdm1.getId();
        assertEquals(expected15, tape1.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected14 = tsdm2.getId();
        assertEquals(expected14, tape2.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected13 = tsdm1.getId();
        assertEquals(expected13, tape3.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected12 = tsdm2.getId();
        assertEquals(expected12, tape4.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected11 = tsdm1.getId();
        assertEquals(expected11, tape5.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected10 = tsdm2.getId();
        assertEquals(expected10, tape6.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        assertEquals(null, tape7.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected9 = psdm1.getId();
        assertEquals(expected9, pool1.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected8 = psdm2.getId();
        assertEquals(expected8, pool2.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected7 = psdm1.getId();
        assertEquals(expected7, pool3.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected6 = psdm2.getId();
        assertEquals(expected6, pool4.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected5 = psdm1.getId();
        assertEquals(expected5, pool5.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        final Object expected4 = psdm2.getId();
        assertEquals(expected4, pool6.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");
        assertEquals(null, pool7.getStorageDomainMemberId(), "Should notta modified storage domain assignments.");

        assertEquals(null, tape1.getBucketId(), "Shoulda fixed standard-isolated buckets with dedicated assignments.");
        assertEquals(null, tape2.getBucketId(), "Shoulda fixed standard-isolated buckets with dedicated assignments.");
        final Object expected3 = bucket2.getId();
        assertEquals(expected3, tape3.getBucketId(), "Should notta 'fixed' non-standard-isolated buckets with dedicated assignments.");
        final Object expected2 = bucket2.getId();
        assertEquals(expected2, tape4.getBucketId(), "Should notta 'fixed' non-standard-isolated buckets with dedicated assignments.");
        assertEquals(null, tape5.getBucketId(), "Should notta modified tapes that didn't have a bucket assignment in the first place.");
        assertEquals(null, tape6.getBucketId(), "Should notta modified tapes that didn't have a bucket assignment in the first place.");
        assertEquals(null, tape7.getBucketId(), "Should notta modified tapes that didn't have a bucket assignment in the first place.");

        assertEquals(null, pool1.getBucketId(), "Shoulda fixed standard-isolated buckets with dedicated assignments.");
        assertEquals(null, pool2.getBucketId(), "Shoulda fixed standard-isolated buckets with dedicated assignments.");
        final Object expected1 = bucket2.getId();
        assertEquals(expected1, pool3.getBucketId(), "Should notta 'fixed' non-standard-isolated buckets with dedicated assignments.");
        final Object expected = bucket2.getId();
        assertEquals(expected, pool4.getBucketId(), "Should notta 'fixed' non-standard-isolated buckets with dedicated assignments.");
        assertEquals(null, pool5.getBucketId(), "Should notta modified pools that didn't have a bucket assignment in the first place.");
        assertEquals(null, pool6.getBucketId(), "Should notta modified pools that didn't have a bucket assignment in the first place.");
        assertEquals(null, pool7.getBucketId(), "Should notta modified pools that didn't have a bucket assignment in the first place.");
    }
    
    
     @Test
    public void testCreateBucketDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID result = resource.createBucket( BeanFactory.newBean( Bucket.class )
                .setDataPolicyId( dp.getId() ).setName( "bucket1" )
                .setUserId( user.getId() ) ).getWithoutBlocking();
        final Object expected = dbSupport.getServiceManager().getRetriever( Bucket.class ).attain(
                Require.nothing() ).getId();
        assertEquals(expected, result, "Shoulda created bucket.");
    }


     @Test
    public void testCreateBucketWithPermanentReplicationRuleDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target dt = mockDaoDriver.createDs3Target( "dt1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, dt.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        final UUID result = resource.createBucket( BeanFactory.newBean( Bucket.class )
                .setDataPolicyId( dp.getId() ).setName( "bucket1" )
                .setUserId( user.getId() ) ).getWithoutBlocking();
        final Object expected = dbSupport.getServiceManager().getRetriever( Bucket.class ).attain(
                Require.nothing() ).getId();
        assertEquals(expected, result, "Shoulda created bucket.");
    }
    
    
     @Test
    public void testCreateBucketWithDataPolicyWithoutPermanentPersistenceRuleNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final AzureTarget at = mockDaoDriver.createAzureTarget( "at1" );
        final Ds3Target dt = mockDaoDriver.createDs3Target( "dt1" );
        final S3Target st = mockDaoDriver.createS3Target( "st1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.TEMPORARY, sd.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, at.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, dt.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, st.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createBucket( BeanFactory.newBean( Bucket.class )
                        .setDataPolicyId( dp.getId() ).setName( "bucket1" )
                        .setUserId( user.getId() ) );
            }
        } );
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyDoesSoWhenIdentical()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyDoesSoWhenCompatible()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean( 
                dp2.setDefaultBlobSize( Long.valueOf( 44444 ) )
                .setDefaultGetJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultPutJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultVerifyJobPriority( BlobStoreTaskPriority.HIGH ).setEndToEndCrcRequired( true )
                .setRebuildPriority( BlobStoreTaskPriority.HIGH ),
                DataPolicy.DEFAULT_BLOB_SIZE, 
                DataPolicy.DEFAULT_GET_JOB_PRIORITY, DataPolicy.DEFAULT_PUT_JOB_PRIORITY, 
                DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY, DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.REBUILD_PRIORITY );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyAllowedWhenSrcContainsDegradedPersistenceRules()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean( 
                dp2.setDefaultBlobSize( Long.valueOf( 44444 ) )
                .setDefaultGetJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultPutJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultVerifyJobPriority( BlobStoreTaskPriority.HIGH ).setEndToEndCrcRequired( true )
                .setRebuildPriority( BlobStoreTaskPriority.HIGH ),
                DataPolicy.DEFAULT_BLOB_SIZE, 
                DataPolicy.DEFAULT_GET_JOB_PRIORITY, DataPolicy.DEFAULT_PUT_JOB_PRIORITY, 
                DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY, DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.REBUILD_PRIORITY );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda retained degraded blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(Require.exists(
                DegradedBlob.PERSISTENCE_RULE_ID,
                Require.beanPropertyEquals(DataPlacement.DATA_POLICY_ID, dp2.getId()))), "Shoulda had degraded blob on dp2 only.");

        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertEquals(true, dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp2 ), "Shoulda allowed full ltfs compliant storage domains in dest since src and dest allowed.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyAllowedWhenDestContainsDegradedDs3ReplicationRules()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean( 
                dp2.setDefaultBlobSize( Long.valueOf( 44444 ) )
                .setDefaultGetJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultPutJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultVerifyJobPriority( BlobStoreTaskPriority.HIGH ).setEndToEndCrcRequired( true )
                .setRebuildPriority( BlobStoreTaskPriority.HIGH ),
                DataPolicy.DEFAULT_BLOB_SIZE, 
                DataPolicy.DEFAULT_GET_JOB_PRIORITY, DataPolicy.DEFAULT_PUT_JOB_PRIORITY, 
                DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY, DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.REBUILD_PRIORITY );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "file:o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda retained degraded blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(Require.exists(
                DegradedBlob.PERSISTENCE_RULE_ID,
                Require.beanPropertyEquals(DataPlacement.DATA_POLICY_ID, dp2.getId()))), "Shoulda had degraded blob on dp2 only.");

        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertEquals(true, dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp2 ), "Shoulda allowed full ltfs compliant storage domains in dest " +
                        "since blob name contains colon.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyAllowedWhenDestContainsDegradedPersistenceRules()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean(
                dp2.setDefaultBlobSize( Long.valueOf( 44444 ) )
                .setDefaultGetJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultPutJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultVerifyJobPriority( BlobStoreTaskPriority.HIGH ).setEndToEndCrcRequired( true )
                .setRebuildPriority( BlobStoreTaskPriority.HIGH ),
                DataPolicy.DEFAULT_BLOB_SIZE,
                DataPolicy.DEFAULT_GET_JOB_PRIORITY, DataPolicy.DEFAULT_PUT_JOB_PRIORITY,
                DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY, DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.REBUILD_PRIORITY );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule =
                mockDaoDriver.createDataPersistenceRule( dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda retained degraded blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(Require.exists(
                DegradedBlob.PERSISTENCE_RULE_ID, Require.beanPropertyEquals(DataPlacement.DATA_POLICY_ID, dp2.getId()))), "Shoulda had degraded blob on dp2 only.");

        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertEquals(true, dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp2 ), "Shoulda allowed full ltfs compliant storage domains in dest since src and dest allowed.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyAllowedWhenSrcContainsDegradedDs3ReplicationRules()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean(
                dp2.setDefaultBlobSize( Long.valueOf( 44444 ) )
                .setDefaultGetJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultPutJobPriority( BlobStoreTaskPriority.LOW )
                .setDefaultVerifyJobPriority( BlobStoreTaskPriority.HIGH ).setEndToEndCrcRequired( true )
                .setRebuildPriority( BlobStoreTaskPriority.HIGH ),
                DataPolicy.DEFAULT_BLOB_SIZE,
                DataPolicy.DEFAULT_GET_JOB_PRIORITY, DataPolicy.DEFAULT_PUT_JOB_PRIORITY,
                DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY, DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.REBUILD_PRIORITY );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3DataReplicationRule rule =
                mockDaoDriver.createDs3DataReplicationRule( dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule( dp2.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(),
                "file/" + String.join( "", Collections.nCopies( 256, "a" ) ) + "/o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda retained degraded blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(Require.exists(
                DegradedBlob.DS3_REPLICATION_RULE_ID,
                Require.beanPropertyEquals(DataPlacement.DATA_POLICY_ID, dp2.getId()))), "Shoulda had degraded blob on dp2 only.");

        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertEquals(false, dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp2 ), "Shoulda disallowed full ltfs compliant storage domains in dest " +
                        "since blob name contains colon.");
    }


     @Test
    public void testModifyBucketModifyingDataPolicyAllowedWhenSrcContainsDegradedS3ReplicationRules()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean(
                dp2.setDefaultBlobSize( Long.valueOf( 44444 ) )
                        .setDefaultGetJobPriority( BlobStoreTaskPriority.LOW )
                        .setDefaultPutJobPriority( BlobStoreTaskPriority.LOW )
                        .setDefaultVerifyJobPriority( BlobStoreTaskPriority.HIGH ).setEndToEndCrcRequired( true )
                        .setRebuildPriority( BlobStoreTaskPriority.HIGH ),
                DataPolicy.DEFAULT_BLOB_SIZE,
                DataPolicy.DEFAULT_GET_JOB_PRIORITY, DataPolicy.DEFAULT_PUT_JOB_PRIORITY,
                DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY, DataPolicy.END_TO_END_CRC_REQUIRED,
                DataPolicy.REBUILD_PRIORITY );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3DataReplicationRule rule =
                mockDaoDriver.createS3DataReplicationRule( dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createS3DataReplicationRule( dp2.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(),
                "file/" + String.join( "", Collections.nCopies( 256, "a" ) ) + "/o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda retained degraded blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(Require.exists(
                DegradedBlob.S3_REPLICATION_RULE_ID,
                Require.beanPropertyEquals(DataPlacement.DATA_POLICY_ID, dp2.getId()))), "Shoulda had degraded blob on dp2 only.");

        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertEquals(false, dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp2 ), "Shoulda disallowed full ltfs compliant storage domains in dest " +
                        "since blob name contains colon.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToIsolationLevel()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD,
                dp.getId(), 
                DataPersistenceRuleType.PERMANENT,
                sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED,
                dp2.getId(),
                DataPersistenceRuleType.PERMANENT, 
                sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToDifferentStorageDomain()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToNumberOfPermRulesMismatch()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToNumOfTempRulesMismatch()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.TEMPORARY, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyWhenNonNormalSrcRuleStateModifiesDestState()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        mockDaoDriver.updateBean( 
                rule.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ), DataPlacement.STATE );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        assertEquals(DataPlacementRuleState.NORMAL, mockDaoDriver.attain( rule2 ).getState(), "Shoulda notta updated destination rule state yet.");

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );

        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( rule ).getState(), "Should notta updated source rule state.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( rule2 ).getState(), "Shoulda updated destination rule state.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyWhenNonNormalDestRuleAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        mockDaoDriver.updateBean( 
                rule2.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ), DataPlacement.STATE );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                dp2.getId() );

        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(DataPlacementRuleState.NORMAL, mockDaoDriver.attain( rule ).getState(), "Should notta updated source rule state.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( rule2 ).getState(), "Shoulda notta updated destination rule state.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToCrcPolicyMismatch()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean( dp2.setChecksumType( ChecksumType.SHA_512 ), DataPolicy.CHECKSUM_TYPE );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }
    
    
     @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToDifferentDs3Targets()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        mockDaoDriver.createDs3DataReplicationRule( 
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule( 
                dp2.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }


     @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToDifferentS3Targets()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3Target sd2 = mockDaoDriver.createS3Target( "sd2" );
        mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dp2.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }


     @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToDifferentAzureTargets()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dp2.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }
    
    @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatible()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        mockDaoDriver.createDs3DataReplicationRule( 
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule( 
                dp2.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule( 
                dp2.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }

    @Test
    public void testModifyBucketModifyingDataPolicyNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dp2.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dp2.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }
    
    @Test
    public void testModifyBucketModifyingDataPolicyNotAllowedWhenIncompatibleDueToNumOfTempDs3RepRulesMismatch()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        mockDaoDriver.createDs3DataReplicationRule( 
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule( 
                dp2.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule( 
                dp2.getId(), DataReplicationRuleType.RETIRED, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyBucket(
                        bucket.getId(),
                        dp2.getId() );
            }
        } );
        final Object expected = dp.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Should notta updated bucket.");
    }
    
    @Test
    public void testModifyBucketModifyingDataPolicyWhenNonNormalDs3SrcRuleStateModifiesDestState()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy srcPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy destPolicy = mockDaoDriver.createDataPolicy( "policy2" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3DataReplicationRule srcRule = mockDaoDriver.createDs3DataReplicationRule( 
                srcPolicy.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Ds3DataReplicationRule destRule = mockDaoDriver.createDs3DataReplicationRule( 
                destPolicy.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), srcPolicy.getId(), "bucket1" );
        mockDaoDriver.updateBean( 
                srcRule.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ), DataPlacement.STATE );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        resource.modifyBucket(
                bucket.getId(),
                destPolicy.getId() );
        final Object expected = destPolicy.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( srcRule ).getState(), "Should notta updated source rule state.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( destRule ).getState(), "Shoulda updated destination rule state.");
    }


     @Test
    public void testModifyBucketModifyingDataPolicyWhenNonNormalS3SrcRuleStateModifiesDestState()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy srcPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy destPolicy = mockDaoDriver.createDataPolicy( "policy2" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3DataReplicationRule srcRule = mockDaoDriver.createS3DataReplicationRule(
                srcPolicy.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final S3DataReplicationRule destRule = mockDaoDriver.createS3DataReplicationRule(
                destPolicy.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), srcPolicy.getId(), "bucket1" );
        mockDaoDriver.updateBean(
                srcRule.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ), DataPlacement.STATE );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        resource.modifyBucket(
                bucket.getId(),
                destPolicy.getId() );
        final Object expected = destPolicy.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( srcRule ).getState(), "Should notta updated source rule state.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( destRule ).getState(), "Shoulda updated destination rule state.");
    }
    
    @Test
    public void testModifyBucketModifyingDataPolicyWhenNonNormalDs3DestRuleAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy srcPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy destPolicy = mockDaoDriver.createDataPolicy( "policy2" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3DataReplicationRule srcRule = mockDaoDriver.createDs3DataReplicationRule( 
                srcPolicy.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Ds3DataReplicationRule destRule = mockDaoDriver.createDs3DataReplicationRule( 
                destPolicy.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), srcPolicy.getId(), "bucket1" );
        mockDaoDriver.updateBean( 
                destRule.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ), DataPlacement.STATE );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyBucket(
                bucket.getId(),
                destPolicy.getId() );
        final Object expected = destPolicy.getId();
        assertEquals(expected, mockDaoDriver.attain( bucket ).getDataPolicyId(), "Shoulda updated bucket.");
        assertEquals(DataPlacementRuleState.NORMAL, mockDaoDriver.attain( srcRule ).getState(), "Should notta updated source rule state.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( destRule ).getState(), "Shoulda notta updated destination rule state.");
    }
    
    
     @Test
    public void testModifyDataPolicyDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPolicy(
                dp.setDefaultBlobSize( Long.valueOf( 44444 ) ),
                new String [] { DataPolicy.DEFAULT_BLOB_SIZE } );
        final Object expected1 = dp.getDefaultBlobSize();
        assertEquals(expected1, mockDaoDriver.attain( dp ).getDefaultBlobSize(), "Shoulda updated data policy.");

        resource.modifyDataPolicy(
                dp.setDefaultBlobSize( null ),
                new String [] { DataPolicy.DEFAULT_BLOB_SIZE } );
        final Object expected = dp.getDefaultBlobSize();
        assertEquals(expected, mockDaoDriver.attain( dp ).getDefaultBlobSize(), "Shoulda updated data policy.");
    }
    
    
     @Test
    public void testModifyDataPolicySettingNotModifiableWhenPolicyInUseAndPolicyNotInUseDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPolicy(
                dp.setDefaultBlobSize( Long.valueOf( 44444 ) ),
                new String [] { DataPolicy.DEFAULT_BLOB_SIZE } );
        final Object expected1 = dp.getDefaultBlobSize();
        assertEquals(expected1, mockDaoDriver.attain( dp ).getDefaultBlobSize(), "Shoulda updated data policy.");

        resource.modifyDataPolicy(
                dp.setChecksumType( ChecksumType.SHA_512 ),
                new String [] { DataPolicy.CHECKSUM_TYPE } );
        final Object expected = dp.getChecksumType();
        assertEquals(expected, mockDaoDriver.attain( dp ).getChecksumType(), "Shoulda updated data policy.");
    }
    
    
     @Test
    public void testModifyDataPolicySettingNotModifiableWhenPolicyInUseAndPolicyInUseNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPolicy(
                dp.setDefaultBlobSize( Long.valueOf( 44444 ) ),
                new String [] { DataPolicy.DEFAULT_BLOB_SIZE } );
        final Object expected1 = dp.getDefaultBlobSize();
        assertEquals(expected1, mockDaoDriver.attain( dp ).getDefaultBlobSize(), "Shoulda updated data policy.");

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPolicy(
                        dp.setChecksumType( ChecksumType.SHA_512 ),
                        new String [] { DataPolicy.CHECKSUM_TYPE } );
            }
        } );
        final Object expected = dp2.getChecksumType();
        assertEquals(expected, mockDaoDriver.attain( dp ).getChecksumType(), "Shoulda updated data policy.");
    }
    
    
     @Test
    public void testModifyDataPolicyDisablingBlobbingAlwaysAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean( dp.setBlobbingEnabled( true ), DataPolicy.BLOBBING_ENABLED );
        mockDaoDriver.updateBean( dp2.setBlobbingEnabled( true ), DataPolicy.BLOBBING_ENABLED );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPolicy(
                dp.setBlobbingEnabled( false ),
                new String [] { DataPolicy.BLOBBING_ENABLED } );
        assertEquals(false, mockDaoDriver.attain( dp ).isBlobbingEnabled(), "Shoulda updated data policy.");

        resource.modifyDataPolicy(
                dp2.setBlobbingEnabled( false ),
                new String [] { DataPolicy.BLOBBING_ENABLED } );
        assertEquals(false, mockDaoDriver.attain( dp2 ).isBlobbingEnabled(), "Shoulda updated data policy.");
    }
    
    
     @Test
    public void testModifyDataPolicyEnablingBlobbingAlwaysAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean( dp.setBlobbingEnabled( false ), DataPolicy.BLOBBING_ENABLED );
        mockDaoDriver.updateBean( dp2.setBlobbingEnabled( false ), DataPolicy.BLOBBING_ENABLED );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPolicy(
                dp.setBlobbingEnabled( true ),
                new String [] { DataPolicy.BLOBBING_ENABLED } );
        assertEquals(true, mockDaoDriver.attain( dp ).isBlobbingEnabled(), "Shoulda updated data policy.");

        resource.modifyDataPolicy(
                dp2.setBlobbingEnabled( true ),
                new String [] { DataPolicy.BLOBBING_ENABLED } );
        assertEquals(true, mockDaoDriver.attain( dp2 ).isBlobbingEnabled(), "Shoulda updated data policy.");
    }
    
    
     @Test
    public void testModifyDataPolicyUpgradingVersioningAllowedIffNoIncompatibleStorageDomainsTargeted()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final DataPolicy dp3 = mockDaoDriver.createDataPolicy( "policy3" );
        mockDaoDriver.updateBean( dp.setBlobbingEnabled( true ), DataPolicy.BLOBBING_ENABLED );
        mockDaoDriver.updateBean( dp2.setBlobbingEnabled( true ), DataPolicy.BLOBBING_ENABLED );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.updateBean( 
                sd2.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ), StorageDomain.LTFS_FILE_NAMING );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp3.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp3.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPolicy(
                dp.setVersioning( VersioningLevel.KEEP_LATEST ),
                new String [] { DataPolicy.VERSIONING } );
        assertEquals(VersioningLevel.KEEP_LATEST, mockDaoDriver.attain( dp ).getVersioning(), "Shoulda updated data policy.");
        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertFalse(dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp ), "Shoulda disallowed object naming LTFS file structure moving forward.");


        resource.modifyDataPolicy(
                dp2.setVersioning( VersioningLevel.KEEP_LATEST ),
                new String [] { DataPolicy.VERSIONING } );
        assertEquals(VersioningLevel.KEEP_LATEST, mockDaoDriver.attain( dp2 ).getVersioning(), "Shoulda updated data policy.");
        assertFalse(dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp2 ), "Shoulda disallowed object naming LTFS file structure moving forward.");

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPolicy(
                        dp3.setVersioning( VersioningLevel.KEEP_LATEST ),
                        new String [] { DataPolicy.VERSIONING } );
            }
        } );
        assertEquals(VersioningLevel.NONE, mockDaoDriver.attain( dp3 ).getVersioning(), "Should notta updated data policy.");
        assertTrue(dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp3 ), "Should notta updated data policy.");
    }
    
    
     @Test
    public void testModifyDataPolicyDowngradingVersioningAllowedOnlyIfDataPolicyNotInUse()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.updateBean( dp.setBlobbingEnabled( true ), DataPolicy.BLOBBING_ENABLED );
        mockDaoDriver.updateBean( dp2.setBlobbingEnabled( true ), DataPolicy.BLOBBING_ENABLED );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPolicy.class ).setVersioning( VersioningLevel.KEEP_LATEST ), 
                DataPolicy.VERSIONING );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPolicy(
                        dp.setVersioning( VersioningLevel.NONE ),
                        new String [] { DataPolicy.VERSIONING } );
            }
        } );
        assertEquals(VersioningLevel.KEEP_LATEST, mockDaoDriver.attain( dp ).getVersioning(), "Shoulda updated data policy.");
        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertTrue(dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp ), "Should notta disallowed object naming LTFS file structure moving forward.");

        resource.modifyDataPolicy(
                dp2.setVersioning( VersioningLevel.NONE ),
                new String [] { DataPolicy.VERSIONING } );
        assertEquals(VersioningLevel.NONE, mockDaoDriver.attain( dp2 ).getVersioning(), "Shoulda updated data policy.");
        assertTrue(dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dp2 ), "Should notta disallowed object naming LTFS file structure moving forward.");
    }
    
    
     @Test
    public void testModifyDataPersistenceRuleSettingDowngradingDataIsolationAlwaysAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED,
                dp.getId(), 
                DataPersistenceRuleType.PERMANENT, 
                sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD,
                dp2.getId(), 
                DataPersistenceRuleType.PERMANENT, 
                sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm =
                mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ).setBucketId( bucket.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPersistenceRule(
                rule1.setIsolationLevel( DataIsolationLevel.STANDARD ),
                new String [] { DataPersistenceRule.ISOLATION_LEVEL } );
        assertEquals(DataIsolationLevel.STANDARD, mockDaoDriver.attain( rule1 ).getIsolationLevel(), "Shoulda updated data policy.");
        assertEquals(null, mockDaoDriver.attain( tape ).getBucketId(), "Shoulda updated persistence targets to standard isolation level.");
    }
    
    
     @Test
    public void testModifyDataPersistenceRuleSettingUpgradingDataIsolationWhenPolicyInUseNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD,
                dp.getId(), 
                DataPersistenceRuleType.PERMANENT,
                sd.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dp2.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPersistenceRule(
                        rule1.setIsolationLevel( DataIsolationLevel.BUCKET_ISOLATED ),
                        new String [] { DataPersistenceRule.ISOLATION_LEVEL } );
            }
        } );
        assertEquals(DataIsolationLevel.STANDARD, mockDaoDriver.attain( rule1 ).getIsolationLevel(), "Should notta updated data policy.");
    }
    
    
     @Test
    public void testModifyDataPersistenceRuleSettingUpgradingDataIsolationFromStdWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD,
                dp.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD,
                dp2.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPersistenceRule(
                rule1.setIsolationLevel( DataIsolationLevel.BUCKET_ISOLATED ),
                new String [] { DataPersistenceRule.ISOLATION_LEVEL } );
        assertEquals(DataIsolationLevel.BUCKET_ISOLATED, mockDaoDriver.attain( rule1 ).getIsolationLevel(), "Shoulda updated data policy.");
    }
    
    
     @Test
    public void testCreateDataPersistenceRulePermWhenTargetStorageDomainContainsNoMembersNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                        .setDataPolicyId( dp.getId() )
                        .setStorageDomainId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataPersistenceRuleType.PERMANENT ) );
            }
        } );
    }
    
    
     @Test
    public void testCreateDataPersistenceRulePermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        final UUID ruleId =
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                    .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                    .setDataPolicyId( dp.getId() )
                    .setStorageDomainId( sd.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataPersistenceRuleType.PERMANENT ) ).getWithoutBlocking();
        mockDaoDriver.attain( DataPersistenceRule.class, ruleId );

        final UUID ruleId2 =
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                    .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                    .setDataPolicyId( dp.getId() )
                    .setStorageDomainId( sd2.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataPersistenceRuleType.PERMANENT ) ).getWithoutBlocking();
        mockDaoDriver.attain( DataPersistenceRule.class, ruleId2 );
    }
    
    
     @Test
    public void testCreateDataPersistenceRulePermWhenPolicyInUseSetToPendingInclusion()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
                
                
        final DataPersistenceRule newRule = BeanFactory.newBean( DataPersistenceRule.class )
        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
        .setDataPolicyId( dp.getId() )
        .setStorageDomainId( sd.getId() )
        .setState( DataPlacementRuleState.NORMAL )
        .setType( DataPersistenceRuleType.PERMANENT );
        
        resource.createDataPersistenceRule( newRule );

        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, newRule.getState(), "New rule should be pending inclusion.");
    }
    
    
     @Test
    public void testCreateDataPersistenceRulePermNotAllowedDuringActivePutJob()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT);
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
            	final DataPersistenceRule newRule = BeanFactory.newBean( DataPersistenceRule.class )
            	        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
            	        .setDataPolicyId( dp.getId() )
            	        .setStorageDomainId( sd.getId() )
            	        .setState( DataPlacementRuleState.NORMAL )
            	        .setType( DataPersistenceRuleType.PERMANENT );
            	resource.createDataPersistenceRule( newRule );
            }
        } );
    }
    
    @Test
    public void testCreateDataPersistenceRulePermWhenIllegalDueToObjectVersioningAndLtfsNamingModeNotAllowed1()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.updateBean( 
                dp.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean(
                sd.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ),
                StorageDomain.LTFS_FILE_NAMING );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                        .setDataPolicyId( dp.getId() )
                        .setStorageDomainId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataPersistenceRuleType.PERMANENT ) );
            }
        } );
        
        mockDaoDriver.updateBean( 
                dp.setVersioning( VersioningLevel.NONE ),
                DataPolicy.VERSIONING );
        resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                .setDataPolicyId( dp.getId() )
                .setStorageDomainId( sd.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataPersistenceRuleType.PERMANENT ) );
    }
    
    @Test
    public void testCreateDataPersistenceRulePermWhenIllegalDueToObjectVersioningAndLtfsNamingModeNotAllowed2()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.updateBean( 
                dp.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean(
                sd.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ),
                StorageDomain.LTFS_FILE_NAMING );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                        .setDataPolicyId( dp.getId() )
                        .setStorageDomainId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataPersistenceRuleType.PERMANENT ) );
            }
        } );

        mockDaoDriver.updateBean(
                sd.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_ID ),
                StorageDomain.LTFS_FILE_NAMING );
        resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                .setDataPolicyId( dp.getId() )
                .setStorageDomainId( sd.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataPersistenceRuleType.PERMANENT ) );
    }
    
    
     @Test
    public void testCreateDataPersistenceRuleTempAgainstPoolWithoutMinDaysToRetainNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), null );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                        .setDataPolicyId( dp.getId() )
                        .setStorageDomainId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataPersistenceRuleType.TEMPORARY ) );
            }
        } );
    }
    
    
     @Test
    public void testCreateDataPersistenceRuleTempAgainstEntPoolWhenPolicyInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "dp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), poolPartition.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                .setDataPolicyId( dp.getId() )
                .setStorageDomainId( sd.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataPersistenceRuleType.TEMPORARY )
                .setMinimumDaysToRetain( Integer.valueOf( 0 ) ) );
    }
    
    
     @Test
    public void testCreateDataPersistenceRuleTempAgainstArchPoolWhenPolicyInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "dp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), poolPartition.getId() );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                        .setDataPolicyId( dp.getId() )
                        .setStorageDomainId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataPersistenceRuleType.TEMPORARY )
                        .setMinimumDaysToRetain( Integer.valueOf( 0 ) ) );
            }
        } );


        resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                .setDataPolicyId( dp.getId() )
                .setStorageDomainId( sd.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataPersistenceRuleType.TEMPORARY )
                .setMinimumDaysToRetain( Integer.valueOf( 10 ) ) );
    }
    
    
     @Test
    public void testCreateDataPersistenceRuleTempAgainstTapeNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                        .setDataPolicyId( dp.getId() )
                        .setStorageDomainId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataPersistenceRuleType.TEMPORARY ) );
            }
        } );
    }
    
    
     @Test
    public void testCreateDataPersistenceRuleRetiredNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                        .setDataPolicyId( dp.getId() )
                        .setStorageDomainId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataPersistenceRuleType.RETIRED ) );
            }
        } );
    }
    
    
     @Test
    public void testCreateDataPersistenceRuleForStorageDomainFullLtfsComplAllowedOnlyIfDataPolicyAllowsIt()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.updateBean(
                sd2.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ), 
                StorageDomain.LTFS_FILE_NAMING );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        final UUID ruleId =
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                    .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                    .setDataPolicyId( dp.getId() )
                    .setStorageDomainId( sd.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataPersistenceRuleType.PERMANENT ) ).getWithoutBlocking();

        final UUID ruleId2 =
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                    .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                    .setDataPolicyId( dp.getId() )
                    .setStorageDomainId( sd2.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataPersistenceRuleType.PERMANENT ) ).getWithoutBlocking();

        resource.deleteDataPersistenceRule( ruleId );
        resource.deleteDataPersistenceRule( ruleId2 );

        resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                .setDataPolicyId( dp.getId() )
                .setStorageDomainId( sd.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataPersistenceRuleType.PERMANENT ) ).getWithoutBlocking();
        
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        mockDaoDriver.createObject( bucket.getId(),
                "file/" + String.join( "", Collections.nCopies( 256, "a" ) ) + "/01" );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDataPersistenceRule( BeanFactory.newBean( DataPersistenceRule.class )
                        .setIsolationLevel( DataIsolationLevel.values()[ 0 ] )
                        .setDataPolicyId( dp.getId() )
                        .setStorageDomainId( sd2.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataPersistenceRuleType.PERMANENT ) ).getWithoutBlocking();
            }
        } );
    }
    
    
     @Test
    public void testModifyDataPersistenceRulePermToNonPermWhenPolicyInUseAllowedIffPermExistsAfter()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyDataPersistenceRule( 
                rule1.setType( DataPersistenceRuleType.RETIRED ), 
                new String [] { DataPersistenceRule.TYPE } );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPersistenceRule( 
                        rule2.setType( DataPersistenceRuleType.RETIRED ), 
                        new String [] { DataPersistenceRule.TYPE } );
            }
        } );
    }

     @Test
    public void testModifyDataPersistenceRulePermToRetiredWhenPolicyInUseAllowedIfInclusionInProgressExistsAfter()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyDataPersistenceRule( 
                rule1.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                new String [] { DataPersistenceRule.STATE } );
        resource.modifyDataPersistenceRule( 
                rule2.setType( DataPersistenceRuleType.RETIRED ), 
                new String [] { DataPersistenceRule.TYPE } );
    }

     @Test
    public void testModifyDataPersistenceRuleRetiredToTempWhenPolicyInUseNotAllowedIfInclusionInProgress()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.RETIRED, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyDataPersistenceRule( 
                rule1.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS  ), 
                new String [] { DataPersistenceRule.STATE } );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPersistenceRule( 
                    rule2.setType( DataPersistenceRuleType.TEMPORARY )
                         .setMinimumDaysToRetain( Integer.valueOf( 180 ) ), 
                    new String [] { DataPersistenceRule.TYPE, DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN } );
            }
        } );
    }
    
    
     @Test
    public void testModifyDataPersistenceRulePermToNonPermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyDataPersistenceRule( 
                rule1.setType( DataPersistenceRuleType.RETIRED ), 
                new String [] { DataPersistenceRule.TYPE } );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Should notta deleted degraded blob yet.");
        resource.modifyDataPersistenceRule( 
                rule2.setType( DataPersistenceRuleType.RETIRED ), 
                new String [] { DataPersistenceRule.TYPE } );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(DegradedBlob.class).getCount(), "Shoulda deleted degraded blob as a result of the type downgrade.");
    }
    
    
     @Test
    public void testModifyDataPersistenceRulePermToTempWhenMinDaysToRetainNotSpecifiedNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), null );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), null );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPersistenceRule( 
                        rule1.setType( DataPersistenceRuleType.TEMPORARY ), 
                        new String [] { DataPersistenceRule.TYPE } );
            }
        } );
    }
    
    
     @Test
    public void testModifyDataPersistenceRulePermToTempWhenTargetingEnterprisePoolAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition poolPartition0 = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "dp" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), poolPartition0.getId() );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "dp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), poolPartition.getId() );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPersistenceRule( 
                rule1.setType( DataPersistenceRuleType.TEMPORARY )
                     .setMinimumDaysToRetain( Integer.valueOf( 0 ) ), 
                new String [] { DataPersistenceRule.TYPE, DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN } );
    }
    
    
     @Test
    public void testModifyDataPersistenceRulePermToTempWhenTargetingArchivePoolAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), null );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "dp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), poolPartition.getId() );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPersistenceRule( 
                        rule1.setType( DataPersistenceRuleType.TEMPORARY )
                             .setMinimumDaysToRetain( Integer.valueOf( 0 ) ), 
                        new String [] { 
                            DataPersistenceRule.TYPE, 
                            DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN } );
            }
        } );

        resource.modifyDataPersistenceRule( 
                rule1.setType( DataPersistenceRuleType.TEMPORARY )
                     .setMinimumDaysToRetain( Integer.valueOf( 10 ) ), 
                new String [] { DataPersistenceRule.TYPE, DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN } );
        
        resource.modifyDataPersistenceRule( 
                rule1.setMinimumDaysToRetain( Integer.valueOf( 122 ) ), 
                new String [] { DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN } );
    }
    
    
     @Test
    public void testModifyDataPersistenceRulePermToTempWhenTargetingTapeNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPersistenceRule( 
                        rule1.setType( DataPersistenceRuleType.TEMPORARY ), 
                        new String [] { DataPersistenceRule.TYPE } );
            }
        } );
    }
    
    
     @Test
    public void testModifyDataPersistenceRuleNonPermToPermWhenPolicyInUseNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.TEMPORARY, sd.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDataPersistenceRule( 
                        rule1.setType( DataPersistenceRuleType.PERMANENT ), 
                        new String [] { DataPersistenceRule.TYPE } );
            }
        } );
    }
    
    
     @Test
    public void testModifyDataPersistenceRuleNonPermToPermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.TEMPORARY, sd.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDataPersistenceRule( 
                rule1.setType( DataPersistenceRuleType.PERMANENT ), 
                new String [] { DataPersistenceRule.TYPE } );
        
        resource.modifyDataPersistenceRule( 
                rule1.setType( DataPersistenceRuleType.TEMPORARY )
                     .setMinimumDaysToRetain( Integer.valueOf( 1 ) ), 
                new String [] { DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN } );
    }
    
    
     @Test
    public void testDeleteDataPersistenceRulePermWhenPolicyInUseNotAllowedIfOnlyUnhealthyPermExistsAfter1()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule1.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.deleteDataPersistenceRule( rule1.getId() );
            }
        } );
    }
    
    
     @Test
    public void testDeleteDataPersistenceRulePermWhenPolicyInUseNotAllowedIfOnlyUnhealthyPermExistsAfter2()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.deleteDataPersistenceRule( rule1.getId() );
            }
        } );
    }
    
    
     @Test
    public void testDeleteDataPersistenceRulePermWhenPolicyInUseAllowedIffHealthyPermExistsAfter()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule1.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteDataPersistenceRule( rule1.getId() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.deleteDataPersistenceRule( rule2.getId() );
            }
        } );
    }
    
    
     @Test
    public void testDeleteDataPersistenceRulePermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), null, TapeType.LTO5 );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), null, TapeType.LTO5 );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final BasicTestsInvocationHandler tapeBtih = new BasicTestsInvocationHandler( null );
        final BasicTestsInvocationHandler poolBtih = new BasicTestsInvocationHandler( null );
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteDataPersistenceRule( rule1.getId() );
        resource.deleteDataPersistenceRule( rule2.getId() );

        assertEquals(0,  tapeBtih.getTotalCallCount(), "Should notta notified blob stores of delete for every valid delete since not in use.");
        assertEquals(0,  poolBtih.getTotalCallCount(), "Should notta notified blob stores of delete for every valid delete since not in use.");
    }
    
    
     @Test
    public void testConvertStorageDomainToDs3TargetWhenFinalPermPersistenceRuleNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "sn1" );
        final StorageDomainMember tsdm1 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        final StorageDomainMember tsdm2 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.updateBean( tp1.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final StorageDomainMember psdm1 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), pool1.getPartitionId() );
        final StorageDomainMember psdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), pool1.getPartitionId() );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( psdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( psdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( tsdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( tsdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, sd2.getId() );
        
        mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b3.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.convertStorageDomainToDs3Target( sd1.getId(), target.getId() );
            }
        } );
    }
    
    
     @Test
    public void testConvertStorageDomainToDs3TargetWhenContainsOnlineTapePartitionMemberNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "sn1" );
        final StorageDomainMember tsdm1 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        final StorageDomainMember tsdm2 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final StorageDomainMember psdm1 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), pool1.getPartitionId() );
        final StorageDomainMember psdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), pool1.getPartitionId() );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( psdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( psdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( tsdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( tsdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        
        mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b3.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.convertStorageDomainToDs3Target( sd1.getId(), target.getId() );
            }
        } );
    }
    
    
     @Test
    public void testConvertStorageDomainToDs3TargetDoesSoWhenPermanentRule()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "sn1" );
        final StorageDomainMember tsdm1 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        final StorageDomainMember tsdm2 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.updateBean( tp1.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final StorageDomainMember psdm1 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), pool1.getPartitionId() );
        final StorageDomainMember psdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), pool1.getPartitionId() );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( psdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( psdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( tsdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( tsdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        
        mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b3.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.convertStorageDomainToDs3Target( sd1.getId(), target.getId() );

        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda created blob targets.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Shoulda whacked blob tapes.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Shoulda whacked blob pools.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DataPersistenceRule.class).getCount(), "Shoulda whacked data persistence rule.");
        assertEquals(DataReplicationRuleType.RETIRED, mockDaoDriver.attainOneAndOnly( Ds3DataReplicationRule.class ).getType(), "Shoulda created data replication rule.");
    }
    
    
     @Test
    public void testConvertStorageDomainToDs3TargetDoesSoWhenNonPermanentRule()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "sn1" );
        final StorageDomainMember tsdm1 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        final StorageDomainMember tsdm2 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.updateBean( tp1.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final StorageDomainMember psdm1 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), pool1.getPartitionId() );
        final StorageDomainMember psdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), pool1.getPartitionId() );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( psdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( psdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( tsdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( tsdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        
        mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b3.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.convertStorageDomainToDs3Target( sd1.getId(), target.getId() );

        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda created blob targets.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Shoulda whacked blob tapes.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Shoulda whacked blob pools.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(DataPersistenceRule.class).getCount(), "Shoulda whacked data persistence rule.");
        assertEquals(DataReplicationRuleType.PERMANENT, mockDaoDriver.attainOneAndOnly( Ds3DataReplicationRule.class ).getType(), "Shoulda created data replication rule.");
    }
    
    
     @Test
    public void testCreateDs3DataReplicationRulePermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        final UUID ruleId =
                resource.createDs3DataReplicationRule( BeanFactory.newBean( Ds3DataReplicationRule.class )
                    .setDataPolicyId( dp.getId() )
                    .setTargetId( sd.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataReplicationRuleType.PERMANENT ) ).getWithoutBlocking();
        mockDaoDriver.attain( Ds3DataReplicationRule.class, ruleId );

        final UUID ruleId2 =
                resource.createDs3DataReplicationRule( BeanFactory.newBean( Ds3DataReplicationRule.class )
                    .setDataPolicyId( dp.getId() )
                    .setTargetId( sd2.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataReplicationRuleType.PERMANENT ) ).getWithoutBlocking();
        mockDaoDriver.attain( Ds3DataReplicationRule.class, ruleId2 );
    }
    
    
     @Test
    public void testCreateDs3DataReplicationRulePermWhenPolicyInUseSetToPendingInclusion()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID ruleId = resource.createDs3DataReplicationRule( BeanFactory.newBean( Ds3DataReplicationRule.class )
                .setDataPolicyId( dp.getId() )
                .setTargetId( sd.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataReplicationRuleType.PERMANENT ) ).get( Timeout.DEFAULT );
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( Ds3DataReplicationRule.class, ruleId).getState(), "Shoulda set rule to inclusion in progress.");
    }
    
    
     @Test
    public void testCreateDs3DataReplicationRulePermNotAllowedDuringActivePutJob()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT);
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
            	resource.createDs3DataReplicationRule( BeanFactory.newBean( Ds3DataReplicationRule.class )
                        .setDataPolicyId( dp.getId() )
                        .setTargetId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataReplicationRuleType.PERMANENT ) ).get( Timeout.DEFAULT );
            }
        } );
    }
    
    
     @Test
    public void testCreateDs3DataReplicationRuleRetiredNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createDs3DataReplicationRule( BeanFactory.newBean( Ds3DataReplicationRule.class )
                        .setDataPolicyId( dp.getId() )
                        .setTargetId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataReplicationRuleType.RETIRED ) );
            }
        } );
    }
    
    
     @Test
    public void testModifyDs3DataReplicationRulePermToNonPermWhenPolicyInUseAllowedIfNotFinal()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        final Ds3DataReplicationRule rule1 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Ds3DataReplicationRule rule2 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyDs3DataReplicationRule( 
                rule1.setType( DataReplicationRuleType.RETIRED ), 
                new String [] { DataReplicationRule.TYPE } );
        TestUtil.assertThrows( null, DataPlannerException.class, new BlastContainer() {
            public void test() throws Throwable {
                resource.modifyDs3DataReplicationRule(
                        rule2.setType(DataReplicationRuleType.RETIRED),
                        new String[]{DataReplicationRule.TYPE});
            }
        });
    }
    
    
     @Test
    public void testModifyDs3DataReplicationRuleNonPermToPermWhenPolicyInUseNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        final Ds3DataReplicationRule rule1 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDs3DataReplicationRule( 
                        rule1.setType( DataReplicationRuleType.PERMANENT ), 
                        new String [] { DataReplicationRule.TYPE } );
            }
        } );
    }
    
    
     @Test
    public void testModifyDs3DataReplicationRuleNonPermToPermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        final Ds3DataReplicationRule rule1 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDs3DataReplicationRule( 
                rule1.setType( DataReplicationRuleType.PERMANENT ), 
                new String [] { DataReplicationRule.TYPE } );
    }
    
    
     @Test
    public void testModifyDs3DataReplicationRuleDs3TargetDataPolicyDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        final Ds3DataReplicationRule rule1 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyDs3DataReplicationRule( 
                rule1.setTargetDataPolicy( "blah" ), 
                new String [] { Ds3DataReplicationRule.TARGET_DATA_POLICY } );
        assertEquals("blah", mockDaoDriver.attain( rule1 ).getTargetDataPolicy(), "Shoulda modified conflict resolution mode.");
    }
    
    
     @Test
    public void testDeleteDs3DataReplicationRulePermWhenPolicyInUseAllowedIfNotFinal()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        final Ds3DataReplicationRule rule1 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Ds3DataReplicationRule rule2 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule1.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteDs3DataReplicationRule( rule1.getId() );
        TestUtil.assertThrows(null, DataPlannerException.class, new BlastContainer() {
        public void test() throws Throwable {
                resource.deleteDs3DataReplicationRule(rule2.getId());
            }
        });
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3DataReplicationRule.class).getCount(), "Shoulda deleted rules.");
    }
    
    
     @Test
    public void testDeleteDs3DataReplicationRulePermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        final Ds3DataReplicationRule rule1 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final Ds3DataReplicationRule rule2 = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteDs3DataReplicationRule( rule1.getId() );
        resource.deleteDs3DataReplicationRule( rule2.getId() );
    }
    
    
     @Test
    public void testCreateAzureDataReplicationRulePermWhenPolicyNotInUseAllowedWhenValidRequest()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        final UUID ruleId =
                resource.createAzureDataReplicationRule( BeanFactory.newBean( AzureDataReplicationRule.class )
                    .setDataPolicyId( dp.getId() )
                    .setTargetId( sd.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataReplicationRuleType.PERMANENT ) ).getWithoutBlocking();
        mockDaoDriver.attain( AzureDataReplicationRule.class, ruleId );

        final UUID ruleId2 =
                resource.createAzureDataReplicationRule( BeanFactory.newBean( AzureDataReplicationRule.class )
                    .setDataPolicyId( dp.getId() )
                    .setTargetId( sd2.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataReplicationRuleType.PERMANENT ) ).getWithoutBlocking();
        mockDaoDriver.attain( AzureDataReplicationRule.class, ruleId2 );
        
        TestUtil.assertThrows(
                "Shoulda thrown exception for bad blob part size", 
                GenericFailure.BAD_REQUEST, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.createAzureDataReplicationRule(
                                BeanFactory.newBean( AzureDataReplicationRule.class )
                                .setMaxBlobPartSizeInBytes( 9999 )
                                .setDataPolicyId( dp.getId() )
                                .setTargetId( sd2.getId() )
                                .setState( DataPlacementRuleState.NORMAL )
                                .setType( DataReplicationRuleType.PERMANENT ) ).getWithoutBlocking();
                    }
                } );
    }
    
    
     @Test
    public void testCreateAzureDataReplicationRulePermWhenPolicyInUseSetToPendingInclusion()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        final UUID ruleId = resource.createAzureDataReplicationRule( BeanFactory.newBean( AzureDataReplicationRule.class )
                .setDataPolicyId( dp.getId() )
                .setTargetId( sd.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataReplicationRuleType.PERMANENT ) ).get( Timeout.DEFAULT );
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( AzureDataReplicationRule.class, ruleId).getState(), "Shoulda set rule to inclusion in progress.");
    }
    
    
     @Test
    public void testCreateAzureDataReplicationRuleRetiredNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createAzureDataReplicationRule( BeanFactory.newBean( AzureDataReplicationRule.class )
                        .setDataPolicyId( dp.getId() )
                        .setTargetId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataReplicationRuleType.RETIRED ) );
            }
        } );
    }
    
    
     @Test
    public void testModifyAzureDataReplicationRulePermToNonPermWhenPolicyInUseAllowedIfNotFinal()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        final AzureDataReplicationRule rule1 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final AzureDataReplicationRule rule2 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyAzureDataReplicationRule( 
                rule1.setType( DataReplicationRuleType.RETIRED ), 
                new String [] { DataReplicationRule.TYPE } );
        TestUtil.assertThrows( null, DataPlannerException.class, new BlastContainer() {
        public void test() throws Throwable {
                resource.modifyAzureDataReplicationRule(
                        rule2.setType(DataReplicationRuleType.RETIRED),
                        new String[]{DataReplicationRule.TYPE});
            }
        });
    }
    
    
     @Test
    public void testModifyAzureDataReplicationRuleNonPermToPermWhenPolicyInUseNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        final AzureDataReplicationRule rule1 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyAzureDataReplicationRule( 
                        rule1.setType( DataReplicationRuleType.PERMANENT ), 
                        new String [] { DataReplicationRule.TYPE } );
            }
        } );
    }
    
    
     @Test
    public void testModifyAzureDataReplicationRuleNonPermToPermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        final AzureDataReplicationRule rule1 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyAzureDataReplicationRule( 
                rule1.setType( DataReplicationRuleType.PERMANENT ), 
                new String [] { DataReplicationRule.TYPE } );
    }
    
    
     @Test
    public void testModifyAzureDataReplicationRuleAzureTargetDataPolicyDoesSoWhenValidRequest()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        final AzureDataReplicationRule rule1 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows(
                "Shoulda thrown exception for illegal max blob part size.",
                GenericFailure.BAD_REQUEST, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.modifyAzureDataReplicationRule( 
                                rule1.setMaxBlobPartSizeInBytes( 99 * 1024 * 1024 ),
                                new String [] { 
                                    PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
                    }
                } );
        TestUtil.assertThrows(
                "Shoulda thrown exception for illegal max blob part size.",
                GenericFailure.BAD_REQUEST, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.modifyAzureDataReplicationRule( 
                                rule1.setMaxBlobPartSizeInBytes( 1024 * 1024L * 1024 * 1024 + 1 ),
                                new String [] { 
                                    PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
                    }
                } );
        
        resource.modifyAzureDataReplicationRule( 
                rule1.setMaxBlobPartSizeInBytes( 999 * 1024 * 1024 ),
                new String [] { PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
        assertEquals(999 * 1024 * 1024,  mockDaoDriver.attain(rule1).getMaxBlobPartSizeInBytes(), "Shoulda modified blob part size.");
        resource.modifyAzureDataReplicationRule( 
                rule1.setMaxBlobPartSizeInBytes( 100 * 1024 * 1024 ),
                new String [] { PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
        resource.modifyAzureDataReplicationRule( 
                rule1.setMaxBlobPartSizeInBytes( 1024 * 1024L * 1024 * 1024 ),
                new String [] { PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
    }
    
    
     @Test
    public void testDeleteAzureDataReplicationRulePermWhenPolicyInUseAllowedIfNotFinal()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        final AzureDataReplicationRule rule1 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final AzureDataReplicationRule rule2 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule1.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteAzureDataReplicationRule( rule1.getId() );
        TestUtil.assertThrows(null, DataPlannerException.class, new BlastContainer() {
            public void test() throws Throwable {
                resource.deleteAzureDataReplicationRule(rule2.getId());
            }
        });
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(AzureDataReplicationRule.class).getCount(), "Shoulda deleted rules.");
    }
    
    
     @Test
    public void testDeleteAzureDataReplicationRulePermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureTarget sd = mockDaoDriver.createAzureTarget( "sd1" );
        final AzureTarget sd2 = mockDaoDriver.createAzureTarget( "sd2" );
        final AzureDataReplicationRule rule1 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final AzureDataReplicationRule rule2 = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteAzureDataReplicationRule( rule1.getId() );
        resource.deleteAzureDataReplicationRule( rule2.getId() );
    }
    
    
     @Test
    public void testCreateS3DataReplicationRulePermWhenPolicyNotInUseAllowedWhenValidRequest()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3Target sd2 = mockDaoDriver.createS3Target( "sd2" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        final UUID ruleId =
                resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                    .setDataPolicyId( dp.getId() )
                    .setTargetId( sd.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataReplicationRuleType.PERMANENT ) ).getWithoutBlocking();
        mockDaoDriver.attain( S3DataReplicationRule.class, ruleId );

        final UUID ruleId2 =
                resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                    .setDataPolicyId( dp.getId() )
                    .setTargetId( sd2.getId() )
                    .setState( DataPlacementRuleState.NORMAL )
                    .setType( DataReplicationRuleType.PERMANENT ) ).getWithoutBlocking();
        mockDaoDriver.attain( S3DataReplicationRule.class, ruleId2 );
        
        TestUtil.assertThrows(
                "Shoulda thrown exception for bad blob part size", 
                GenericFailure.BAD_REQUEST, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.createS3DataReplicationRule(
                                BeanFactory.newBean( S3DataReplicationRule.class )
                                .setMaxBlobPartSizeInBytes( 9999 )
                                .setDataPolicyId( dp.getId() )
                                .setTargetId( sd2.getId() )
                                .setState( DataPlacementRuleState.NORMAL )
                                .setType( DataReplicationRuleType.PERMANENT ) ).getWithoutBlocking();
                    }
                } );
    }
    
    
     @Test
    public void testCreateS3DataReplicationRulePermWhenPolicyInUseSetToPendingInclusion()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        mockDaoDriver.createBucket( user.getId(), dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        final UUID ruleId = resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                .setDataPolicyId( dp.getId() )
                .setTargetId( sd.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataReplicationRuleType.PERMANENT ) ).get( Timeout.DEFAULT );
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain( S3DataReplicationRule.class, ruleId).getState(), "Shoulda set rule to inclusion in progress.");
    }
    
    
     @Test
    public void testRedundantReplicationToAWSNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target t1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target t2 = mockDaoDriver.createS3Target( "t2" );
        final S3Target t3 = mockDaoDriver.createS3Target( "t3" );
        final S3Target t4 = mockDaoDriver.createS3Target( "t4" );
        final S3Target t5 = mockDaoDriver.createS3Target( "t5" );
        mockDaoDriver.updateBean(
                t1.setRegion( S3Region.US_WEST_1 ).setDataPathEndPoint( null ).setAccessKey("a1"),
                S3Target.REGION, S3Target.DATA_PATH_END_POINT, S3Target.ACCESS_KEY );
        mockDaoDriver.updateBean(
                t2.setRegion( S3Region.US_WEST_1 ).setDataPathEndPoint( "e" ).setAccessKey("a1"),
                S3Target.REGION, S3Target.DATA_PATH_END_POINT, S3Target.ACCESS_KEY );
        mockDaoDriver.updateBean(
                t3.setRegion( S3Region.US_WEST_1 ).setDataPathEndPoint( null )
                    .setAccessKey("a1").setCloudBucketPrefix("prefix"),
                S3Target.REGION, S3Target.DATA_PATH_END_POINT, S3Target.ACCESS_KEY,
                PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX );
        mockDaoDriver.updateBean(
                t4.setRegion( S3Region.US_WEST_1 ).setDataPathEndPoint( null )
                    .setAccessKey("a1").setCloudBucketSuffix("suffix"),
                S3Target.REGION, S3Target.DATA_PATH_END_POINT, S3Target.ACCESS_KEY,
                PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX );
        mockDaoDriver.updateBean(
                t5.setRegion( S3Region.EU_CENTRAL_1 ).setDataPathEndPoint( null ).setAccessKey("a2"),
                S3Target.REGION, S3Target.DATA_PATH_END_POINT, S3Target.ACCESS_KEY );
        
        
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
                
        resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                .setDataPolicyId( dp.getId() )
                .setTargetId( t1.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataReplicationRuleType.PERMANENT ) );
                
        resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                .setDataPolicyId( dp.getId() )
                .setTargetId( t2.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataReplicationRuleType.PERMANENT ) );
                
        resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                .setDataPolicyId( dp.getId() )
                .setTargetId( t3.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataReplicationRuleType.PERMANENT ) );
                
        resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                .setDataPolicyId( dp.getId() )
                .setTargetId( t4.getId() )
                .setState( DataPlacementRuleState.NORMAL )
                .setType( DataReplicationRuleType.PERMANENT ) );

        TestUtil.assertThrows(
                "Should notta allowed creation of second AWS replication rule on the same data policy.",
                GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                        .setDataPolicyId( dp.getId() )
                        .setTargetId( t5.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataReplicationRuleType.PERMANENT ) );
            }
        } );
    }
    
    
     @Test
    public void testCreateS3DataReplicationRuleRetiredNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createS3DataReplicationRule( BeanFactory.newBean( S3DataReplicationRule.class )
                        .setDataPolicyId( dp.getId() )
                        .setTargetId( sd.getId() )
                        .setState( DataPlacementRuleState.NORMAL )
                        .setType( DataReplicationRuleType.RETIRED ) );
            }
        } );
    }
    
    
     @Test
    public void testModifyS3DataReplicationRulePermToNonPermWhenPolicyInUseAllowedIfNotFinal()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3Target sd2 = mockDaoDriver.createS3Target( "sd2" );
        final S3DataReplicationRule rule1 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final S3DataReplicationRule rule2 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyS3DataReplicationRule(
                rule1.setType( DataReplicationRuleType.RETIRED ),
                new String [] { DataReplicationRule.TYPE } );

        TestUtil.assertThrows( null, DataPlannerException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyS3DataReplicationRule(
                        rule2.setType( DataReplicationRuleType.RETIRED ),
                        new String [] { DataReplicationRule.TYPE } );
            }
        } );

        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd3.getId() );

        resource.modifyS3DataReplicationRule(
                rule2.setType( DataReplicationRuleType.RETIRED ),
                new String [] { DataReplicationRule.TYPE } );
    }
    
    
     @Test
    public void testModifyS3DataReplicationRuleNonPermToPermWhenPolicyInUseNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3Target sd2 = mockDaoDriver.createS3Target( "sd2" );
        final S3DataReplicationRule rule1 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyS3DataReplicationRule( 
                        rule1.setType( DataReplicationRuleType.PERMANENT ), 
                        new String [] { DataReplicationRule.TYPE } );
            }
        } );
    }
    
    
     @Test
    public void testModifyS3DataReplicationRuleNonPermToPermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3Target sd2 = mockDaoDriver.createS3Target( "sd2" );
        final S3DataReplicationRule rule1 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyS3DataReplicationRule( 
                rule1.setType( DataReplicationRuleType.PERMANENT ), 
                new String [] { DataReplicationRule.TYPE } );
    }
    
    
     @Test
    public void testModifyS3DataReplicationRuleS3TargetDataPolicyDoesSoWhenValidRequest()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3Target sd2 = mockDaoDriver.createS3Target( "sd2" );
        final S3DataReplicationRule rule1 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.RETIRED, sd.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows(
                "Shoulda thrown exception for illegal max blob part size.",
                GenericFailure.BAD_REQUEST, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.modifyS3DataReplicationRule( 
                                rule1.setMaxBlobPartSizeInBytes( 99 * 1024 * 1024 ),
                                new String [] { 
                                    PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
                    }
                } );
        TestUtil.assertThrows(
                "Shoulda thrown exception for illegal max blob part size.",
                GenericFailure.BAD_REQUEST, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.modifyS3DataReplicationRule( 
                                rule1.setMaxBlobPartSizeInBytes( 1024 * 1024L * 1024 * 1024 + 1 ),
                                new String [] { 
                                    PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
                    }
                } );
        
        resource.modifyS3DataReplicationRule( 
                rule1.setMaxBlobPartSizeInBytes( 999 * 1024 * 1024 ),
                new String [] { PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
        assertEquals(999 * 1024 * 1024,  mockDaoDriver.attain(rule1).getMaxBlobPartSizeInBytes(), "Shoulda modified blob part size.");
        resource.modifyS3DataReplicationRule( 
                rule1.setMaxBlobPartSizeInBytes( 100 * 1024 * 1024 ),
                new String [] { PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
        resource.modifyS3DataReplicationRule( 
                rule1.setMaxBlobPartSizeInBytes( 1024 * 1024L * 1024 * 1024 ),
                new String [] { PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES } );
    }
    
    
     @Test
    public void testDeleteS3DataReplicationRulePermWhenPolicyInUseAllowedIfNotFinal()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3Target sd2 = mockDaoDriver.createS3Target( "sd2" );
        final S3DataReplicationRule rule1 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final S3DataReplicationRule rule2 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.createDegradedBlob( blob.getId(), rule1.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteS3DataReplicationRule( rule1.getId() );
        TestUtil.assertThrows(null, DataPlannerException.class, new BlastContainer() {
        public void test() throws Throwable {
                resource.deleteS3DataReplicationRule(rule2.getId());
            }
        });
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3DataReplicationRule.class).getCount(), "Shoulda deleted rules.");
    }
    
    
     @Test
    public void testDeleteS3DataReplicationRulePermWhenPolicyNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final S3Target sd = mockDaoDriver.createS3Target( "sd1" );
        final S3Target sd2 = mockDaoDriver.createS3Target( "sd2" );
        final S3DataReplicationRule rule1 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd.getId() );
        final S3DataReplicationRule rule2 = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, sd2.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteS3DataReplicationRule( rule1.getId() );
        resource.deleteS3DataReplicationRule( rule2.getId() );
    }
    
    
     @Test
    public void testModifyStorageDomainLtfsCompatibilityLevelWhenNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        resource.modifyStorageDomain(
                sd.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ),
                new String [] { StorageDomain.LTFS_FILE_NAMING } );
        assertEquals(LtfsFileNamingMode.OBJECT_NAME, mockDaoDriver.attain( sd ).getLtfsFileNaming(), "Shoulda modified storage domain.");
    }
    
    @Test
    public void testModifyStorageDomainLtfsCompatibilityLevelWhenNotInUseExceptDataPolicyWithVersioningNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.updateBean( dp.setVersioning( VersioningLevel.KEEP_LATEST ), DataPolicy.VERSIONING );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyStorageDomain(
                        sd.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ),
                        new String [] { StorageDomain.LTFS_FILE_NAMING } );
            }
        } );
        assertEquals(LtfsFileNamingMode.OBJECT_ID, mockDaoDriver.attain( sd ).getLtfsFileNaming(), "Should notta modified storage domain.");
    }
    
    
     @Test
    public void testModifyStorageDomainButNotLtfsCompatibilityLevelWhenInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        resource.modifyStorageDomain(
                sd,
                new String [] { StorageDomain.WRITE_OPTIMIZATION } );
    }
    
    
     @Test
    public void testModifyStorageDomainLtfsCompatibilityLevelWhenInUseDueToTapeAllocationNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm =
                mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyStorageDomain(
                        sd.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ),
                        new String [] { StorageDomain.LTFS_FILE_NAMING } );
            }
        } );
        assertEquals(LtfsFileNamingMode.OBJECT_ID, mockDaoDriver.attain( sd ).getLtfsFileNaming(), "Should notta modified storage domain.");
    }
    
    
     @Test
    public void testModifyStorageDomainLtfsCompatibilityLevelWhenInUseDueToBucketNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule( 
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        
        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyStorageDomain(
                        sd.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ),
                        new String [] { StorageDomain.LTFS_FILE_NAMING } );
            }
        } );
        assertEquals(LtfsFileNamingMode.OBJECT_ID, mockDaoDriver.attain( sd ).getLtfsFileNaming(), "Should notta modified storage domain.");
    }
    
    
     @Test
    public void testCreateStorageDomainMemberForTapeWhenCannotWriteToTargetResultsInAutoWritePrefUpdate()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( partition.setDriveType( TapeDriveType.LTO7 ), TapePartition.DRIVE_TYPE );
        final StorageDomainMember member1 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setTapeType( TapeType.LTO5 ).setTapePartitionId( partition.getId() );
        final StorageDomainMember member2 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setTapeType( TapeType.LTO6 ).setTapePartitionId( partition.getId() );
                        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.createDataPersistenceRule(
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.createStorageDomainMember( member1 );
        resource.createStorageDomainMember( member2 );
        assertEquals(WritePreferenceLevel.NEVER_SELECT, mockDaoDriver.attain( member1 ).getWritePreference(), "Shoulda auto-reset write preference to read only since it is read only.");
        assertEquals(WritePreferenceLevel.HIGH, mockDaoDriver.attain( member2 ).getWritePreference(), "Should notta auto-reset write preference to read only since it wasn't read only.");
    }
    
    
     @Test
    public void testCreateStorageDomainMemberForTapeWhenStorageDomainNotTargetedAsTempPersistenceAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setTapeType( TapeType.LTO5 ).setTapePartitionId(
                        mockDaoDriver.createTapePartition( null, "p1" ).getId() );
        final StorageDomainMember member2 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setTapeType( TapeType.LTO5 ).setTapePartitionId(
                        mockDaoDriver.createTapePartition( null, "p2" ).getId() );
                        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.createDataPersistenceRule(
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.createStorageDomainMember( member1 );
        resource.createStorageDomainMember( member2 );
    }
    
    
     @Test
    public void testCreateStorageDomainMemberForTapeWhenStorageDomainTargetedAsTempPersistenceNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setTapeType( TapeType.LTO5 ).setTapePartitionId(
                        mockDaoDriver.createTapePartition( null, "p1" ).getId() );
        final StorageDomainMember member2 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setTapeType( TapeType.LTO5 ).setTapePartitionId(
                        mockDaoDriver.createTapePartition( null, "p2" ).getId() );
                        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.createDataPersistenceRule(
                dp2.getId(), DataPersistenceRuleType.TEMPORARY, sd.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createStorageDomainMember( member1 );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createStorageDomainMember( member2 );
            }
        } );
    }
    
    
     @Test
    public void testCreateStorageDomainMemberForTapeWhenTapeTypeNullOrInvalidNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setTapeType( null ).setTapePartitionId(
                        mockDaoDriver.createTapePartition( null, "p1" ).getId() );
        final StorageDomainMember member2 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setTapeType( TapeType.LTO_CLEANING_TAPE ).setTapePartitionId(
                        mockDaoDriver.createTapePartition( null, "p2" ).getId() );
                        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.createDataPersistenceRule(
                dp2.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createStorageDomainMember( member1 );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createStorageDomainMember( member2 );
            }
        } );
        
        member2.setTapeType( TapeType.UNKNOWN );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createStorageDomainMember( member2 );
            }
        } );
    }
    
    
     @Test
    public void testCreateStorageDomainMemberForPoolDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setPoolPartitionId(
                        mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p1" ).getId() );
        final StorageDomainMember member2 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setPoolPartitionId(
                        mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p2" ).getId() );
                        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.createDataPersistenceRule(
                dp2.getId(), DataPersistenceRuleType.TEMPORARY, sd.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.createStorageDomainMember( member1 );
        resource.createStorageDomainMember( member2 );
    }
    
    
     @Test
    public void testCreateStorageDomainMemberForPoolWhenAdditionWouldViolateMinRetentionNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setPoolPartitionId(
                        mockDaoDriver.createPoolPartition( PoolType.ONLINE, "p1" ).getId() );
        final StorageDomainMember member2 = BeanFactory.newBean( StorageDomainMember.class )
                .setStorageDomainId( sd.getId() ).setWritePreference( WritePreferenceLevel.values()[ 0 ] )
                .setPoolPartitionId(
                        mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "p2" ).getId() );
                        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "policy2" );
        final DataPersistenceRule tempRule = mockDaoDriver.createDataPersistenceRule(
                dp2.getId(), DataPersistenceRuleType.TEMPORARY, sd.getId() );
        mockDaoDriver.updateBean( 
                tempRule.setMinimumDaysToRetain( Integer.valueOf( 0 ) ),
                DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.createStorageDomainMember( member1 );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createStorageDomainMember( member2 );
            }
        } );
    }
    
    
     @Test
    public void testModifyStorageDomainMemberForTapeWhenCannotWriteToTargetResultsInAutoWritePreferenceSet()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "tp1" );
        mockDaoDriver.updateBean( partition.setDriveType( TapeDriveType.LTO6 ), TapePartition.DRIVE_TYPE );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                partition.getId(),
                TapeType.LTO7 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                partition.getId(),
                TapeType.LTO5 );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyStorageDomainMember( member1.setWritePreference( WritePreferenceLevel.LOW ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
        resource.modifyStorageDomainMember( member2.setWritePreference( WritePreferenceLevel.LOW ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );

        assertEquals(WritePreferenceLevel.NEVER_SELECT, mockDaoDriver.attain( member1 ).getWritePreference(), "Shoulda auto-reset write preference to read only since it is read only.");
        assertEquals(WritePreferenceLevel.LOW, mockDaoDriver.attain( member2 ).getWritePreference(), "Should notta auto-reset write preference to read only since it wasn't read only.");
    }
    
    
     @Test
    public void testModifyStorageDomainMemberReadOnlyToNonReadOnlyWhenStorageDomainNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                mockDaoDriver.createTapePartition( null, "tp1" ).getId(),
                TapeType.LTO5 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO5 );
        mockDaoDriver.updateBean(
                member1.setWritePreference( WritePreferenceLevel.NEVER_SELECT ), 
                StorageDomainMember.WRITE_PREFERENCE );
        mockDaoDriver.updateBean(
                member2.setWritePreference( WritePreferenceLevel.NEVER_SELECT ), 
                StorageDomainMember.WRITE_PREFERENCE );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyStorageDomainMember( member1.setWritePreference( WritePreferenceLevel.NORMAL ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
        resource.modifyStorageDomainMember( member2.setWritePreference( WritePreferenceLevel.NORMAL ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
    }
    
    
     @Test
    public void testModifyStorageDomainMemberReadOnlyToNonReadOnlyWhenStorageDomainInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                mockDaoDriver.createTapePartition( null, "tp1" ).getId(),
                TapeType.LTO5 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO5 );
        mockDaoDriver.updateBean(
                member1.setWritePreference( WritePreferenceLevel.NEVER_SELECT ), 
                StorageDomainMember.WRITE_PREFERENCE );
        mockDaoDriver.updateBean(
                member2.setWritePreference( WritePreferenceLevel.NEVER_SELECT ), 
                StorageDomainMember.WRITE_PREFERENCE );
        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyStorageDomainMember( member1.setWritePreference( WritePreferenceLevel.NORMAL ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
        resource.modifyStorageDomainMember( member2.setWritePreference( WritePreferenceLevel.NORMAL ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
    }
    
    
     @Test
    public void testModifyStorageDomainMemberNonReadOnlyToReadOnlyWhenStorageDomainNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                mockDaoDriver.createTapePartition( null, "tp1" ).getId(),
                TapeType.LTO5 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO5 );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyStorageDomainMember( member1.setWritePreference( WritePreferenceLevel.NEVER_SELECT ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
        resource.modifyStorageDomainMember( member2.setWritePreference( WritePreferenceLevel.NEVER_SELECT ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
    }
    
    @Test
    public void testExcludeOrNeverSelectMemberWhenStorageDomainInUseAllowedIffNonReadOnlyRemains()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                mockDaoDriver.createTapePartition( null, "tp1" ).getId(),
                TapeType.LTO6 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO6 );
        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyStorageDomainMember( member1.setWritePreference( WritePreferenceLevel.NEVER_SELECT ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyStorageDomainMember( member2.setWritePreference( WritePreferenceLevel.NEVER_SELECT ),
                new String[] { StorageDomainMember.WRITE_PREFERENCE } );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyStorageDomainMember( member2.setState( StorageDomainMemberState.EXCLUSION_IN_PROGRESS ),
                        new String[] { StorageDomainMember.STATE } );
            }
        } );
    }
    
    
     @Test
    public void testDeleteStorageDomainMemberWhenStorageDomainNotInUseAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                mockDaoDriver.createTapePartition( null, "tp1" ).getId(),
                TapeType.LTO5 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO5 );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteStorageDomainMember( member1.getId() );
        resource.deleteStorageDomainMember( member2.getId() );
    }
    
    
     @Test
    public void testDeleteStorageDomainMemberWhenStorageDomainInUseAllowedIffNonReadOnlyRemains()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                mockDaoDriver.createTapePartition( null, "tp1" ).getId(),
                TapeType.LTO5 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO5 );
        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.deleteStorageDomainMember( member1.getId() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.deleteStorageDomainMember( member2.getId() );
            }
        } );
    }
    
    
     @Test
    public void testDeleteStorageDomainMemberWhenTapeAllocatedToStorageDomainAllowedIfStorageDomainNotInUse()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                tp1.getId(),
                TapeType.LTO5 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO5 );
        
        mockDaoDriver.updateBean(
                mockDaoDriver.createTape( tp1.getId(), TapeState.NORMAL, TapeType.LTO5 )
                    .setStorageDomainMemberId( member1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.deleteStorageDomainMember( member1.getId() );
        resource.deleteStorageDomainMember( member2.getId() );
    }
    
    
     @Test
    public void testDeleteStorageDomainMemberWhenTapeAllocatedToStorageDomainAllowedWhenNoBlobsOnTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                tp1.getId(),
                TapeType.LTO5 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO5 );
        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPersistenceRule( dp.getId(), null, sd.getId() );
        
        mockDaoDriver.updateBean(
                mockDaoDriver.createTape( tp1.getId(), TapeState.NORMAL, TapeType.LTO5 )
                    .setStorageDomainMemberId( member1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.deleteStorageDomainMember( member1.getId() );

        assertEquals(null, mockDaoDriver.retrieve( member1 ), "Rule shoulda been deleted.");

        assertEquals(null, mockDaoDriver.attainOneAndOnly( Tape.class ).getStorageDomainMemberId(), "Tape shoulda lost sdm assignment.");

        TestUtil.assertThrows( "Should notta been allowed to delete storage domain member.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.deleteStorageDomainMember( member2.getId() );
                    }
                } );
    }
    
    
     @Test
    public void testDeleteStorageDomainMemberNotAllowedWhenDataOnAllocatedTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final Bucket bucket = mockDaoDriver.createBucket( null , "bucket" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "object" );
        final Blob b1 = mockDaoDriver.getBlobFor( object.getId() );
        final StorageDomainMember member1 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(),
                tp1.getId(),
                TapeType.LTO5 );
        final StorageDomainMember member2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createTapePartition( null, "tp2" ).getId(),
                TapeType.LTO5 );
        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPersistenceRule( dp.getId(), null, sd.getId() );
        
        final Tape tape = mockDaoDriver.createTape( tp1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        
        mockDaoDriver.putBlobOnTape( tape.getId(), b1.getId() );
        mockDaoDriver.updateBean(
                tape.setStorageDomainMemberId( member1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyStorageDomainMember(
        		member1.setState( StorageDomainMemberState.EXCLUSION_IN_PROGRESS ),
        		new String[] { StorageDomainMember.STATE });

        assertEquals(StorageDomainMemberState.EXCLUSION_IN_PROGRESS, mockDaoDriver.attain( member1 ).getState(), "Rule should be pending exclusion following delete.");

        TestUtil.assertThrows( "Should notta been allowed to delete storage domain member.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.deleteStorageDomainMember( member2.getId() );
                    }
                } );
    }
    
    
     @Test
    public void testDeleteStorageDomainMemberWhenPoolAllocatedToStorageDomainAllowedIfStorageDomainNotInUse()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "tp1" );
        final StorageDomainMember member1 = mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(),
                dp1.getId() );
        final StorageDomainMember member2 = mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createPoolPartition( null, "tp2" ).getId() );
        
        mockDaoDriver.updateBean(
                mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL ).setStorageDomainMemberId( member1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.deleteStorageDomainMember( member1.getId() );
        resource.deleteStorageDomainMember( member2.getId() );
    }
    
    
     @Test
    public void testDeleteStorageDomainMemberWhenPoolAllocatedToStorageDomainAllowedWhenNoBlobsOnPool()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "tp1" );
        final StorageDomainMember member1 = mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(),
                dp1.getId() );
        final StorageDomainMember member2 = mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createPoolPartition( null, "tp2" ).getId() );
        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPersistenceRule( dp.getId(), null, sd.getId() );
        
        mockDaoDriver.updateBean(
                mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL ).setStorageDomainMemberId( member1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.deleteStorageDomainMember( member1.getId() );

        assertEquals(null, mockDaoDriver.retrieve( member1 ), "Rule shoulda been deleted.");

        assertEquals(null, mockDaoDriver.attainOneAndOnly( Pool.class ).getStorageDomainMemberId(), "Pool shoulda lost sdm assignment.");

        TestUtil.assertThrows( "Should notta been allowed to delete last storage domain member.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.deleteStorageDomainMember( member2.getId() );
                    }
                } );
    }
    
    
     @Test
    public void testDeleteStorageDomainMemberNotAllowedWhenDataOnAllocatedPool()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "tp1" );
        final Bucket bucket = mockDaoDriver.createBucket( null , "bucket" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "object" );
        final Blob b1 = mockDaoDriver.getBlobFor( object.getId() );
        final StorageDomainMember member1 = mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(),
                dp1.getId() );
        final StorageDomainMember member2 = mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(), 
                mockDaoDriver.createPoolPartition( null, "tp2" ).getId() );
        
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPersistenceRule( dp.getId(), null, sd.getId() );
        
        final Pool pool = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean(
                pool.setStorageDomainMemberId( member1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
                
        mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        resource.modifyStorageDomainMember(
        		member1.setState( StorageDomainMemberState.EXCLUSION_IN_PROGRESS ),
        		new String[] { StorageDomainMember.STATE });

        assertEquals(StorageDomainMemberState.EXCLUSION_IN_PROGRESS, mockDaoDriver.attain( member1 ).getState(), "Rule should be pending exclusion following delete.");

        TestUtil.assertThrows( "Should notta been allowed to delete storage domain member.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.deleteStorageDomainMember( member2.getId() );
                    }
                } );
    }
    
    
     @Test
    public void testModifyPoolToNullPartitionWherePoolNotAssignedToStorageDomainAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "tp1" );
        final PoolPartition dp2 = mockDaoDriver.createPoolPartition( null, "tp2" );
        mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(),
                dp1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(), 
                dp2.getId() );
        final Pool pool = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyPool( pool.getId(), dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( pool ).getPartitionId(), "Shoulda moved pool to specified partition.");

        resource.modifyPool( pool.getId(), null );
        assertEquals(null, mockDaoDriver.attain( pool ).getPartitionId(), "Shoulda moved pool to specified partition.");
    }
    
    
     @Test
    public void testModifyPoolWherePoolNotAssignedToStorageDomainAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "tp1" );
        final PoolPartition dp2 = mockDaoDriver.createPoolPartition( null, "tp2" );
        final PoolPartition dp3 = mockDaoDriver.createPoolPartition( null, "tp3" );
        mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(),
                dp1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(), 
                dp2.getId() );
        final Pool pool = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyPool( pool.getId(), dp2.getId() );
        final Object expected1 = dp2.getId();
        assertEquals(expected1, mockDaoDriver.attain( pool ).getPartitionId(), "Shoulda moved pool to specified partition.");

        resource.modifyPool( pool.getId(), dp3.getId() );
        final Object expected = dp3.getId();
        assertEquals(expected, mockDaoDriver.attain( pool ).getPartitionId(), "Shoulda moved pool to specified partition.");
    }
    
    
     @Test
    public void testModifyPoolWherePartitionWrongTypeNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "tp1" );
        final PoolPartition dp2 = mockDaoDriver.createPoolPartition( null, "tp2" );
        mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(),
                dp1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(), 
                dp2.getId() );
        final Pool pool = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool.setType( PoolType.ONLINE ), PoolObservable.TYPE );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyPool( pool.getId(), dp2.getId() );
            }
        } );
        final Object expected1 = dp1.getId();
        assertEquals(expected1, mockDaoDriver.attain( pool ).getPartitionId(), "Should notta moved pool to specified partition.");

        mockDaoDriver.updateBean( pool.setType( PoolType.NEARLINE ), PoolObservable.TYPE );
        resource.modifyPool( pool.getId(), dp2.getId() );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( pool ).getPartitionId(), "Shoulda moved pool to specified partition.");
    }
    
    
     @Test
    public void testModifyPoolWherePoolAssignedToStorageDomainOnlyAllowedIfDestInStorageDomain()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "tp1" );
        final PoolPartition dp2 = mockDaoDriver.createPoolPartition( null, "tp2" );
        final PoolPartition dp3 = mockDaoDriver.createPoolPartition( null, "tp3" );
        final PoolPartition dp4 = mockDaoDriver.createPoolPartition( null, "tp4" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(),
                dp1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd.getId(), 
                dp2.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( 
                sd2.getId(), 
                dp4.getId() );
        final Pool pool = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean(
                pool.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        final DataPolicyManagementResource resource = new DataPolicyManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
        
        resource.modifyPool( pool.getId(), dp2.getId() );
        final Object expected3 = dp2.getId();
        assertEquals(expected3, mockDaoDriver.attain( pool ).getPartitionId(), "Shoulda moved pool to specified partition.");

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyPool( pool.getId(), dp3.getId() );
            }
        } );
        final Object expected2 = dp2.getId();
        assertEquals(expected2, mockDaoDriver.attain( pool ).getPartitionId(), "Should notta moved pool to specified partition.");

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyPool( pool.getId(), dp4.getId() );
            }
        } );
        final Object expected1 = dp2.getId();
        assertEquals(expected1, mockDaoDriver.attain( pool ).getPartitionId(), "Should notta moved pool to specified partition.");
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyPool( pool.getId(), null );
            }
        } );
        final Object expected = dp2.getId();
        assertEquals(expected, mockDaoDriver.attain( pool ).getPartitionId(), "Should notta moved pool to specified partition.");
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
