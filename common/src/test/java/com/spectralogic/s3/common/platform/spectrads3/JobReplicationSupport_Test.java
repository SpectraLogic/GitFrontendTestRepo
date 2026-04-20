/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.spectrads3;

import java.lang.reflect.Array;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobChunkToReplicate;
import com.spectralogic.s3.common.rpc.dataplanner.domain.PersistenceTargetInfo;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class JobReplicationSupport_Test 
{

    @Test
    public void testCreateJobToReplicateFromExistingJobNullJobNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
    public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        (UUID)null );
            }
        } );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromAggregatingJobNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        
        final MockSupport ms = new MockSupport( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.updateBean(
                ms.m_job.setAggregating( true ),
                Job.AGGREGATING );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        ms.m_job.getId() );
            }
        } );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobThatHasPartiallyCompletedAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        
        final MockSupport ms = new MockSupport( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.updateBean(
                ms.m_job.setCompletedSizeInBytes( 1 ),
                JobObservable.COMPLETED_SIZE_IN_BYTES );
        new JobReplicationSupport( 
                serviceManager, 
                ms.m_job.getId() );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromExistingNonPutJobAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        
        final MockSupport ms = new MockSupport( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.updateBean(
                ms.m_job.setRequestType( JobRequestType.GET ),
                JobObservable.REQUEST_TYPE );
        new JobReplicationSupport( 
                serviceManager, 
                ms.m_job.getId() );
        mockDaoDriver.updateBean(
                ms.m_job.setRequestType( JobRequestType.VERIFY ),
                JobObservable.REQUEST_TYPE );
        new JobReplicationSupport( 
                serviceManager, 
                ms.m_job.getId() );
        mockDaoDriver.updateBean(
                ms.m_job.setRequestType( JobRequestType.PUT ),
                JobObservable.REQUEST_TYPE );
        new JobReplicationSupport( 
                serviceManager, 
                ms.m_job.getId() );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromExistingJobDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        
        final MockSupport ms = new MockSupport( dbSupport );
        final JobReplicationSupport support = new JobReplicationSupport( 
                serviceManager, 
                ms.m_job.getId() );
        final Object expected9 = BeanUtils.< UUID >extractPropertyValues(
                ms.getBlobs(),
                Identifiable.ID );
        assertEquals(expected9, BeanUtils.<UUID>extractPropertyValues(
                        support.getBlobs(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected8 = BeanUtils.< UUID >extractPropertyValues(
                ms.getObjects(),
                Identifiable.ID );
        assertEquals(expected8, BeanUtils.<UUID>extractPropertyValues(
                        support.getObjects(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected7 = BeanUtils.< UUID >extractPropertyValues(
                ms.getChunks(),
                Identifiable.ID );
        assertEquals(expected7, BeanUtils.<UUID>extractPropertyValues(
                        support.getJobChunks(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        assertEquals(4,  support.getJobChunks().size(), "Shoulda computed support correctly.");

        final DetailedJobToReplicate retval = support.getJobToReplicate();
        final Object expected6 = ms.m_user.getId();
        assertEquals(expected6, retval.getUserId(), "Shoulda computed support correctly.");
        final Object expected5 = ms.m_bucket.getId();
        assertEquals(expected5, retval.getBucketId(), "Shoulda computed support correctly.");
        final Object expected4 = ms.m_job.getPriority();
        assertEquals(expected4, retval.getPriority(), "Shoulda computed support correctly.");
        final Object expected3 = ms.m_job.getId();
        assertEquals(expected3, retval.getJob().getId(), "Shoulda computed support correctly.");
        final Object expected2 = BeanUtils.< UUID >extractPropertyValues(
                ms.getBlobs(),
                Identifiable.ID );
        assertEquals(expected2, BeanUtils.<UUID>extractPropertyValues(
                        CollectionFactory.toSet( retval.getJob().getBlobs() ),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected1 = BeanUtils.< UUID >extractPropertyValues(
                ms.getObjects(),
                Identifiable.ID );
        assertEquals(expected1, BeanUtils.<UUID>extractPropertyValues(
                        CollectionFactory.toSet( retval.getJob().getObjects() ),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected = BeanUtils.< UUID >extractPropertyValues(
                ms.getChunks(),
                Identifiable.ID );
        assertEquals(expected, BeanUtils.<UUID>extractPropertyValues(
                        CollectionFactory.toSet( retval.getJob().getChunks() ),
                        Identifiable.ID ), "Shoulda computed support correctly.");

        JobChunkToReplicate c1 = retval.getJob().getChunks()[ 0 ];
        JobChunkToReplicate c2 = retval.getJob().getChunks()[ 1 ];
        if ( c2.getId().equals( ms.m_chunk1.getId() ) )
        {
            final JobChunkToReplicate temp = c1;
            c1 = c2;
            c2 = temp;
        }

        assertEquals(retval.getJob().getChunks().length,  support.getJobChunks().size(), "Shoulda has the same number of entries.");
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateNullParamsNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );

        ms.getJobToReplicate().setPriority( null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        null,
                        ms.getJobToReplicate() );
            }
        } );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager,
                        (DetailedJobToReplicate)null );
            }
        } );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

    public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager,
                        ms.getJobToReplicate().setUserId( null ) );
            }
        } );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

    public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager,
                        ms.getJobToReplicate().setBucketId( null ) );
            }
        } );
        
        new JobReplicationSupport( 
                serviceManager,
                ms.getJobToReplicate() );
        
        new JobReplicationSupport( 
                serviceManager,
                ms.getJobToReplicate().setPriority( BlobStoreTaskPriority.HIGH ) );
    }
    
    
    @Test
    public void testCommitWhenSomeBlobsPreviouslyAllocatedThrowsException()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        
        final UUID allocatedBlobId = ms.getBlobs().iterator().next().getId();
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.allocateChunksForBlob( allocatedBlobId );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        mockDaoDriver.createObject( bucket.getId(), "o4", 33333 );

        TestUtil.assertThrows(
        		"Shoulda thrown 503 since on re-replication since blobs still allocated in cache",
        		GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
        		new BlastContainer()
        {

    public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        jobToReplicate ).commit( cacheManager, createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) );
            }
        } );
    }
    
    
    @Test
    public void testCommitWhenNoOtherVersionsAndVersioningModeNoneAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        mockDaoDriver.createObject( bucket.getId(), "o4", 33333 );

        new JobReplicationSupport( 
                serviceManager, 
                jobToReplicate ).commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) );
    }
    
    
    @Test
    public void testCommitWhenSomeObjectsAlreadyExistResultsInThoseObjectsIgnored()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );

        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );

        new JobReplicationSupport(
                serviceManager,
                jobToReplicate ).commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) );
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda ignored o1 and b1.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Shoulda ignored o1 and b1.");
    }
    
    
    @Test
    public void testCommitWhenOtherVersionsAndVersioningModeNoneNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 33333 );
        mockDaoDriver.updateBean( object.setCreationDate( new Date() ), S3Object.CREATION_DATE );

        TestUtil.assertThrows( null, AWSFailure.OBJECT_ALREADY_EXISTS, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        jobToReplicate ).commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) );
            }
        } );
    }
    
    
    @Test
    public void testCommitWhenOtherVersionsAndVersioningModeKeepLatestAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        mockDaoDriver.createObject( bucket.getId(), "o1", 33333 );
        
        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( DataPolicy.class ).setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        
        new JobReplicationSupport( 
                serviceManager, 
                jobToReplicate ).commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(Require.all(
                Require.beanPropertyEquals(S3Object.NAME, "o1"),
                Require.beanPropertyEquals(S3Object.LATEST, Boolean.TRUE))), "Shoulda retained only one latest value.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.NAME, "o1")), "Shoulda retained only one latest value.");
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateDoesSoWhenNothingWasExisting()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        final JobReplicationSupport support = new JobReplicationSupport( 
                serviceManager, 
                jobToReplicate );
        final Object expected2 = BeanUtils.< UUID >extractPropertyValues(
                ms.getBlobs(),
                Identifiable.ID );
        assertEquals(expected2, BeanUtils.<UUID>extractPropertyValues(
                        support.getBlobs(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected1 = BeanUtils.< UUID >extractPropertyValues(
                ms.getObjects(),
                Identifiable.ID );
        assertEquals(expected1, BeanUtils.<UUID>extractPropertyValues(
                        support.getObjects(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected = BeanUtils.< UUID >extractPropertyValues(
                ms.getChunks(),
                Identifiable.ID );
        assertEquals(expected, BeanUtils.<UUID>extractPropertyValues(
                        support.getJobChunks(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        assertEquals(4,  support.getJobChunks().size(), "Shoulda computed support correctly.");
        assertEquals(111,  support.getJob().getOriginalSizeInBytes(), "Shoulda computed support correctly.");

        assertNotNull(
                support.commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) ),
                "Creating job shoulda been necessary."
                 );
        ms.verifyCommitted(4, 111 );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateDoesSoWhenSomeObjectsWhereExisting()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );
        
        final JobReplicationSupport support = new JobReplicationSupport(
                serviceManager,
                jobToReplicate );
        final Object expected2 = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet(
                        ms.m_b2, ms.m_b3, ms.m_b4 ),
                Identifiable.ID );
        assertEquals(expected2, BeanUtils.<UUID>extractPropertyValues(
                        support.getBlobs(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected1 = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet( ms.m_o2, ms.m_o3 ),
                Identifiable.ID );
        assertEquals(expected1, BeanUtils.<UUID>extractPropertyValues(
                        support.getObjects(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected = BeanUtils.< UUID >extractPropertyValues(
                Set.of(ms.m_chunk2, ms.m_chunk3, ms.m_chunk4),
                Identifiable.ID );
        assertEquals(expected, BeanUtils.<UUID>extractPropertyValues(
                        support.getJobChunks(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        assertEquals(3,  support.getJobChunks().size(), "Shoulda computed support correctly.");
        assertEquals(111,  support.getJob().getOriginalSizeInBytes(), "Shoulda computed support correctly.");

        assertNotNull(
                support.commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) ),
                "Creating job shoulda been necessary."
               );
        ms.verifyCommitted(3, 111 );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateDoesSoWhenEntireChunkOfObjectsExisting()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );
        
        ms.m_o2.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o2 );
        dbSupport.getDataManager().createBean( ms.m_b2 );

        final JobReplicationSupport support = new JobReplicationSupport(
                serviceManager,
                jobToReplicate );
        final Object expected2 = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet(
                        ms.m_b3, ms.m_b4 ),
                Identifiable.ID );
        assertEquals(expected2, BeanUtils.<UUID>extractPropertyValues(
                        support.getBlobs(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected1 = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet( ms.m_o3 ),
                Identifiable.ID );
        assertEquals(expected1, BeanUtils.<UUID>extractPropertyValues(
                        support.getObjects(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet( ms.m_chunk3, ms.m_chunk4),
                Identifiable.ID );
        assertEquals(expected, BeanUtils.<UUID>extractPropertyValues(
                        support.getJobChunks(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        assertEquals(2,  support.getJobChunks().size(), "Shoulda computed support correctly.");
        assertEquals(100,  support.getJob().getOriginalSizeInBytes(), "Shoulda computed support correctly.");

        assertNotNull(
                support.commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) ),
                "Creating job shoulda been necessary."
                 );
        ms.verifyCommitted(2, 100 );
    }


    @Test
    public void testCreateJobToReplicateFromJobToReplicateDoesSoWhenEntireChunkOfObjectsExistingAndPrevVers()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );
        
        ms.m_o2.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o2 );
        dbSupport.getDataManager().createBean( ms.m_b2 );
        
        final S3Object otherO1 = mockDaoDriver.createObject( bucket.getId(), "o1", 1111 );
        final S3Object otherO2 = mockDaoDriver.createObject( bucket.getId(), "o2", 1111 );

        final JobReplicationSupport support = new JobReplicationSupport(
                serviceManager,
                jobToReplicate );
        final Object expected2 = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet(
                        ms.m_b3, ms.m_b4 ),
                Identifiable.ID );
        assertEquals(expected2, BeanUtils.<UUID>extractPropertyValues(
                        support.getBlobs(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected1 = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet( ms.m_o3 ),
                Identifiable.ID );
        assertEquals(expected1, BeanUtils.<UUID>extractPropertyValues(
                        support.getObjects(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet( ms.m_chunk3, ms.m_chunk4),
                Identifiable.ID );
        assertEquals(expected, BeanUtils.<UUID>extractPropertyValues(
                        support.getJobChunks(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        assertEquals(2,  support.getJobChunks().size(), "Shoulda computed support correctly.");
        assertEquals(100,  support.getJob().getOriginalSizeInBytes(), "Shoulda computed support correctly.");

        assertNotNull(
                support.commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport)),
                        "Creating job shoulda been necessary."
                );
        ms.verifyCommitted(2, 100, otherO1, otherO2 );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateDoesSoWhenAllChunksExisting()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );
        
        ms.m_o2.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o2 );
        dbSupport.getDataManager().createBean( ms.m_b2 );
        
        ms.m_o3.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o3 );
        dbSupport.getDataManager().createBean( ms.m_b3 );
        dbSupport.getDataManager().createBean( ms.m_b4 );
        
        final JobReplicationSupport support = new JobReplicationSupport( 
                serviceManager, 
                jobToReplicate );
        final Object expected2 = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet(),
                Identifiable.ID );
        assertEquals(expected2, BeanUtils.<UUID>extractPropertyValues(
                        support.getBlobs(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected1 = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet(),
                Identifiable.ID );
        assertEquals(expected1, BeanUtils.<UUID>extractPropertyValues(
                        support.getObjects(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        final Object expected = BeanUtils.< UUID >extractPropertyValues(
                CollectionFactory.toSet(),
                Identifiable.ID );
        assertEquals(expected, BeanUtils.<UUID>extractPropertyValues(
                        support.getJobChunks(),
                        Identifiable.ID ), "Shoulda computed support correctly.");
        assertEquals(0,  support.getJobChunks().size(), "Shoulda computed support correctly.");
        assertEquals(0,  support.getJob().getOriginalSizeInBytes(), "Shoulda computed support correctly.");

        assertNull(
                support.commit( new MockDiskManager( dbSupport.getServiceManager() ), createPersistenceTargetInfo(bucket.getDataPolicyId(), dbSupport) ),
                "Creating job should notta been necessary."
               );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Job.class).getCount(), "Creating job should notta been necessary.");
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateNotAllowedIfObjectsSpanMultipleBuckets()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        jobToReplicate.getJob().getObjects()[ 0 ].setBucketId( UUID.randomUUID() );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        jobToReplicate );
            }
        } );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateNotAllowedIfMissingAllBlobs()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        jobToReplicate.getJob().setBlobs( (Blob[])Array.newInstance( Blob.class, 0 ) );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        ms.m_o1.setId( UUID.randomUUID() );
        ms.m_b1.setObjectId( ms.m_o1.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        jobToReplicate );
            }
        } );
    }
    

    @Test
    public void testCreateJobToReplicateFromJobToReplicateNotAllowedIfOverlappingBlobInSequence()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        for ( final Blob blob : jobToReplicate.getJob().getBlobs() )
        {
            if ( 0 == blob.getByteOffset() )
            {
                blob.setLength( blob.getLength() + 1 );
            }
        }
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        jobToReplicate );
            }
        } );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateNotAllowedIfDiscontiguousBlobInSequence()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        for ( final Blob blob : jobToReplicate.getJob().getBlobs() )
        {
            if ( blob.getId().equals( ms.m_b1.getId() ) )
            {
                continue;
            }
            blob.setByteOffset( blob.getByteOffset() + 1 );
            break;
        }
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        jobToReplicate );
            }
        } );
    }
    
    
    @Test
    public void testCreateJobToReplicateFromJobToReplicateNotAllowedIfExtraBlobInSequence()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockSupport ms = new MockSupport( dbSupport );
        
        final DetailedJobToReplicate jobToReplicate = ms.getJobToReplicate();
        final Blob [] arrayBlobs = jobToReplicate.getJob().getBlobs();
        final Blob [] newArrayBlobs = new Blob[ arrayBlobs.length + 1 ];
        System.arraycopy( arrayBlobs, 0, newArrayBlobs, 0, arrayBlobs.length );
        newArrayBlobs[ arrayBlobs.length ] = (Blob)BeanFactory.newBean( Blob.class )
                .setByteOffset( ms.m_b1.getLength() ).setLength( 10 ).setObjectId( ms.m_b1.getObjectId() )
                .setId( UUID.randomUUID() );
        jobToReplicate.getJob().setBlobs( newArrayBlobs );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Bucket.class );
        mockDaoDriver.deleteAll( User.class );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "someone" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.createBucket( user.getId(), "other" );
        jobToReplicate.setUserId( user.getId() ).setBucketId( bucket.getId() );
        
        ms.m_o1.setBucketId( bucket.getId() );
        dbSupport.getDataManager().createBean( ms.m_o1 );
        dbSupport.getDataManager().createBean( ms.m_b1 );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobReplicationSupport( 
                        serviceManager, 
                        jobToReplicate );
            }
        } );
    }
    
    
    private final static class MockSupport
    {
        private MockSupport( final DatabaseSupport dbSupport )
        {
            m_mockDaoDriver = new MockDaoDriver( dbSupport );
            m_serviceManager = dbSupport.getServiceManager();
            m_serviceManager.getService( BucketService.class ).initializeLogicalSizeCache();
            
            m_user = m_mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
            m_bucket = m_mockDaoDriver.createBucket( m_user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
            m_o1 = m_mockDaoDriver.createObject( m_bucket.getId(), "o1", 0 );
            m_b1 = m_mockDaoDriver.getBlobFor( m_o1.getId() );
            m_o2 = m_mockDaoDriver.createObject( m_bucket.getId(), "o2", 11 );
            m_b2 = m_mockDaoDriver.getBlobFor( m_o2.getId() );
            m_o3 = m_mockDaoDriver.createObject( m_bucket.getId(), "o3", -1 );
            final var blobs = m_mockDaoDriver.createBlobs( m_o3.getId(), 2, 50 );
            m_b3 = blobs.get(0);
            m_b4 = blobs.get(1);

            m_job = m_mockDaoDriver.createJob( m_bucket.getId(), m_user.getId(), JobRequestType.PUT );
            m_chunk1 = m_mockDaoDriver.createJobEntry( m_job.getId(), m_b1 );
            m_chunk2 = m_mockDaoDriver.createJobEntry( m_job.getId(), m_b2 );
            m_chunk3 = m_mockDaoDriver.createJobEntry( m_job.getId(), m_b3 );
            m_chunk4 = m_mockDaoDriver.createJobEntry( m_job.getId(), m_b4 );
        }
        
        
        private Set< Blob > getBlobs()
        {
            return CollectionFactory.toSet( m_b1, m_b2, m_b3, m_b4 );
        }


        private Set< S3Object > getObjects()
        {
            return CollectionFactory.toSet( m_o1, m_o2, m_o3 );
        }


        private Set<JobEntry> getChunks()
        {
            return CollectionFactory.toSet( m_chunk1, m_chunk2, m_chunk3, m_chunk4);
        }
        
        
        private DetailedJobToReplicate getJobToReplicate()
        {
            return new JobReplicationSupport( m_serviceManager, m_job.getId() ).getJobToReplicate();
        }
        
        private void verifyCommitted(
                final int numJobEntriesExpected,
                final long jobSizeExpected,
                final S3Object ... extraObjects )
        {
            final Set< S3Object > extraObjectsSet = CollectionFactory.toSet( extraObjects );
            final Set< S3Object > objects = getObjects();
            final Set< Blob > blobs = getBlobs();
            objects.addAll( extraObjectsSet );
            if ( !extraObjectsSet.isEmpty() )
            {
                blobs.addAll( m_serviceManager.getRetriever( Blob.class ).retrieveAll( 
                        Require.beanPropertyEqualsOneOf(
                                Blob.OBJECT_ID,
                                BeanUtils.< UUID >extractPropertyValues(
                                        extraObjectsSet, Identifiable.ID ) ) ).toSet() );
            }

            final Object expected1 = BeanUtils.< UUID >extractPropertyValues(
                    blobs,
                    Identifiable.ID );
            assertEquals(expected1, BeanUtils.<UUID>extractPropertyValues(
                                m_serviceManager.getRetriever( Blob.class ).retrieveAll().toSet(),
                                Identifiable.ID ), "Shoulda computed support correctly.");
            final Object expected = BeanUtils.< UUID >extractPropertyValues(
                    objects,
                    Identifiable.ID );
            assertEquals(expected, BeanUtils.<UUID>extractPropertyValues(
                                m_serviceManager.getRetriever( S3Object.class ).retrieveAll().toSet(),
                                Identifiable.ID ), "Shoulda computed support correctly.");
            assertEquals(numJobEntriesExpected,  m_serviceManager.getRetriever(JobEntry.class).getCount(), "Shoulda computed support correctly.");
            assertEquals(jobSizeExpected,  m_serviceManager.getRetriever(Job.class).retrieve(Require.nothing())
                    .getOriginalSizeInBytes(), "Shoulda computed support correctly.");
            assertTrue(
                    m_serviceManager.getRetriever( Job.class ).retrieve( Require.nothing() )
                            .isReplicating(),
                    "Shoulda computed support correctly."
                   );
        }
        
        
        private final User m_user;
        private final Bucket m_bucket;
        private final S3Object m_o1;
        private final S3Object m_o2;
        private final S3Object m_o3;
        private final Blob m_b1;
        private final Blob m_b2;
        private final Blob m_b3;
        private final Blob m_b4;
        private final Job m_job;
        private final JobEntry m_chunk1;
        private final JobEntry m_chunk2;
        private final JobEntry m_chunk3;
        private final JobEntry m_chunk4;
        private final BeansServiceManager m_serviceManager;
        private final MockDaoDriver m_mockDaoDriver;
    } // end inner class def

    private PersistenceTargetInfo createPersistenceTargetInfo(final UUID dataPolicyId, final DatabaseSupport dbSupport) {
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockDaoDriver driver = new MockDaoDriver(dbSupport);
        driver.createStorageDomain("sd1");
        final Set<UUID> storageDomainIds = BeanUtils.toMap(serviceManager.getRetriever(StorageDomain.class).retrieveAll().toSet()).keySet();
        final PersistenceTargetInfo pti = BeanFactory.newBean(PersistenceTargetInfo.class);
        pti.setStorageDomainIds(CollectionFactory.toArray(UUID.class, storageDomainIds));
        return pti;
    }
}
