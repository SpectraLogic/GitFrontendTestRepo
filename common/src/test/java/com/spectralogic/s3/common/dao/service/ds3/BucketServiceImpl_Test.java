/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.aws.S3Utils;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import java.util.UUID;

public final class BucketServiceImpl_Test 
{
    @Test
    public void testCreateWhenBucketNameAlreadyExistsNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dpolicy" );

        final BucketService service = dbSupport.getServiceManager().getService( BucketService.class );

        final Bucket bucket = BeanFactory.newBean( Bucket.class );
        bucket.setDataPolicyId( dataPolicy.getId() );
        bucket.setUserId( user.getId() );
        bucket.setName( "bucket_name" );
        service.create( bucket );

        final Bucket bucketThatFails = BeanFactory.newBean( Bucket.class );
        bucketThatFails.setUserId( user.getId() );
        bucketThatFails.setName( "bucket_name" );
        TestUtil.assertThrows(
                "Shoulda thrown a conflict failure.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        service.create( bucketThatFails );
                    }
                } );
    }
    
    
    @Test
    public void testCreateRequiresValidBucketName()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dpolicy" );
        
        final BucketService service = dbSupport.getServiceManager().getService( BucketService.class );
        final Bucket bucket = BeanFactory.newBean( Bucket.class )
                .setDataPolicyId( dataPolicy.getId() ).setUserId( user.getId() )
                .setName( S3Utils.REST_REQUEST_REQUIRED_PREFIX );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( bucket );
                }
            } );
        
        bucket.setName( bucket.getName().toLowerCase() );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( bucket );
                }
            } );
        
        bucket.setName( bucket.getName().toUpperCase() );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( bucket );
                }
            } );
        
        bucket.setName( "$" );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( bucket );
                }
            } );
        
        bucket.setName( "#" );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( bucket );
                }
            } );
        
        bucket.setName( "" );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( bucket );
                }
            } );
        
        bucket.setName( null );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( bucket );
                }
            } );
        
        bucket.setName( "+" );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( bucket );
                }
            } );
        
        bucket.setName( "some_valid-name.0" );
        service.create( bucket );
        bucket.setId( null );
        
        bucket.setName( "a" );
        service.create( bucket );
        bucket.setId( null );
        
        bucket.setName( "0" );
        service.create( bucket );
        bucket.setId( null );
        
        bucket.setName( "_" );
        service.create( bucket );
        
        final BucketAcl acl = dbSupport.getServiceManager().getRetriever( BucketAcl.class ).attain(
                BucketAcl.BUCKET_ID, bucket.getId() );
        final Object expected = bucket.getUserId();
        assertEquals(expected, acl.getUserId(), "Shoulda auto-generated ownership acl.");
        assertEquals(BucketAclPermission.OWNER, acl.getPermission(), "Shoulda auto-generated ownership acl.");
    }
    
    
    @Test
    public void testUpdateWhereUserIdGetsUpdatedCreatesNewImplicitOwnerAcl()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp" );
        final User user = mockDaoDriver.createUser( "user" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        mockDaoDriver.createBucket( null, "someotherbucket" );

        final Bucket bucket = BeanFactory.newBean( Bucket.class )
                .setName( "bucket" ).setUserId( user.getId() ).setDataPolicyId( dataPolicy.getId() );
        final BucketService service = dbSupport.getServiceManager().getService( BucketService.class );
        service.create( bucket );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BucketAcl.class).getCount(
                Require.beanPropertyEquals(BucketAcl.BUCKET_ID, bucket.getId())), "Shoulda generated implicit ownership acl.");
        service.update( bucket, NameObservable.NAME, UserIdObservable.USER_ID );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BucketAcl.class).getCount(
                Require.beanPropertyEquals(BucketAcl.BUCKET_ID, bucket.getId())), "Should notta generated another implicit ownership acl.");
        bucket.setUserId( user2.getId() );
        service.update( bucket, NameObservable.NAME );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BucketAcl.class).getCount(
                Require.beanPropertyEquals(BucketAcl.BUCKET_ID, bucket.getId())), "Should notta generated another implicit ownership acl.");
        service.update( bucket, NameObservable.NAME, UserIdObservable.USER_ID );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(BucketAcl.class).getCount(
                Require.beanPropertyEquals(BucketAcl.BUCKET_ID, bucket.getId())), "Shoulda generated another implicit ownership acl.");
    }
    
    
    @Test
    public void testGetLogicalCapacityDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final BucketService service = dbSupport.getServiceManager().getService( BucketService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "a" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b" );
        
        mockDaoDriver.createObject( bucket1.getId(), "o1", 1 );
        mockDaoDriver.createObject( bucket1.getId(), "o2", 10 );
        mockDaoDriver.createObject( bucket2.getId(), "o3", 100 );

        assertEquals(11,  service.getLogicalCapacity(bucket1.getId()), "Shoulda reported correct size.");
        assertEquals(100,  service.getLogicalCapacity(bucket2.getId()), "Shoulda reported correct size.");
    }
    
    
    @Test
    public void testGetPendingPutWorkInBytesReturnsCorrectValue()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Bucket b = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        final Bucket b2 = mockDaoDriver.createBucket( null, dp.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( b.getId(), "o1", 1 );
        final S3Object o2 = mockDaoDriver.createObject( b.getId(), "o2", 10 );
        final S3Object o3 = mockDaoDriver.createObject( b.getId(), "o3", 100 );
        final S3Object o4 = mockDaoDriver.createObject( b.getId(), "o4", 1000 );
        final S3Object o5 = mockDaoDriver.createObject( b.getId(), "o5", 10000 );
        mockDaoDriver.createObject( b.getId(), "o6", 100000 );
        mockDaoDriver.createObject( b2.getId(), "o7", 1000000 );
        final S3Object o8 = mockDaoDriver.createObject( b2.getId(), "o8", 10000000 );
        
        final Job job1 = mockDaoDriver.createJob( b.getId(), null, JobRequestType.PUT );
        final Job job2 = mockDaoDriver.createJob( b.getId(), null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( b.getId(), null, JobRequestType.PUT );
        final Job job4 = mockDaoDriver.createJob( b2.getId(), null, JobRequestType.PUT );

        final UUID storageDomainId = mockDaoDriver.attainOneAndOnly(StorageDomain.class).getId();
        
        final JobEntry chunk1 = mockDaoDriver.createJobEntry(job1.getId(),
                mockDaoDriver.getBlobFor( o1.getId() ) );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry(job2.getId(),
                mockDaoDriver.getBlobFor( o2.getId() ) );
        final JobEntry chunk31 = mockDaoDriver.createJobEntry(job3.getId(),
                mockDaoDriver.getBlobFor( o3.getId() ) );
        final JobEntry chunk32 = mockDaoDriver.createJobEntry(job3.getId(),
                mockDaoDriver.getBlobFor( o4.getId() ) );
        final JobEntry chunk33 = mockDaoDriver.createJobEntry(job3.getId(),
                mockDaoDriver.getBlobFor( o5.getId() ) );
        final JobEntry chunk4 = mockDaoDriver.createJobEntry(job4.getId(),
                mockDaoDriver.getBlobFor( o8.getId() ) );


        mockDaoDriver.createPersistenceTargetsForChunk(chunk1.getId());
        mockDaoDriver.createPersistenceTargetsForChunk(chunk31.getId());
        mockDaoDriver.createPersistenceTargetsForChunk(chunk32.getId());
        //NOTE: we do not create targets for chunk33, so it should not be counted
        mockDaoDriver.createPersistenceTargetsForChunk(chunk4.getId());

        assertEquals(1101,  dbSupport.getServiceManager().getService(BucketService.class).getPendingPutWorkInBytes(
                b.getId(), storageDomainId), "Shoulda included all pending blobs in PUT jobs for bucket.");
        assertEquals(10000000,  dbSupport.getServiceManager().getService(BucketService.class).getPendingPutWorkInBytes(
                b2.getId(), storageDomainId), "Shoulda included all pending blobs in PUT jobs for bucket.");

        assertEquals(10001101,  dbSupport.getServiceManager().getService(BucketService.class).getPendingPutWorkInBytes(
                null, storageDomainId), "Shoulda included all pending blobs in PUT jobs for storage domain.");
    }
    
    
    @Test
    public void testGetLockReturnsSameLockRegardlessAsToWhetherInsideTransaction()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        assertSame(
                dbSupport.getServiceManager().getService( BucketService.class ).getLock(),
                transaction.getService( BucketService.class ).getLock(),
                "Transaction shoulda delegated to source to get the lock instance."
                 );
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testGetLogicalSizeCacheThrowsUntilInitialized()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BucketService service = dbSupport.getServiceManager().getService( BucketService.class );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    service.getLogicalSizeCache();
                }
            } );
        
        service.initializeLogicalSizeCache();

        assertNotNull(service.getLogicalSizeCache(), "Shoulda returned logical size cache.");
    }
}
