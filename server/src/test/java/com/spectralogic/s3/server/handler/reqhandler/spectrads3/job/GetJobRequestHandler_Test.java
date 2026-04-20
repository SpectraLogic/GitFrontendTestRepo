/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;

public final class GetJobRequestHandler_Test 
{
    @Test
    public void testGetJobInfoReturnsCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final UUID jobId = 
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b ).getJobId();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        driver.assertResponseToClientContains( "IN_PROGRESS" );
    }
    
    
    @Test
    public void testGetJobInfoReturnsEntirelyInCacheCorrectlyForVerifyJobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final UUID jobId = mockDaoDriver.createJobWithEntry(
                JobRequestType.VERIFY, b ).getJobId();
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( JobEntry.class ).setBlobStoreState( JobChunkBlobStoreState.COMPLETED ),
                JobEntry.BLOB_STORE_STATE );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        driver.assertResponseToClientContains( "IN_PROGRESS" );
    }
    
    
    @Test
    public void testGetJobInfoReturnsEntirelyInCacheCorrectlyForGetJobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final UUID jobId = mockDaoDriver.createJobWithEntry(
                JobRequestType.GET, b ).getJobId();

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        
        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( JobEntry.class ).setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                JobEntry.BLOB_STORE_STATE );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        
        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( JobEntry.class ).setBlobStoreState( JobChunkBlobStoreState.COMPLETED ),
                JobEntry.BLOB_STORE_STATE );
        final File file = cacheFilesystemDriver.writeCacheFile( b.getId(), 100 );
        file.deleteOnExit();
        final BlobCache bc = BeanFactory.newBean(BlobCache.class)
                .setBlobId(b.getId())
                .setState(CacheEntryState.IN_CACHE)
                .setLastAccessed(new Date(System.currentTimeMillis()))
                . setPath(file.getPath());

        dbSupport.getDataManager().createBean( bc );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"true\"" );
    }


    @Test
    public void testEntirelyInCacheCorrectlyForPartialEntries()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1", -1 );
        List<Blob> blobs = mockDaoDriver.createBlobs(o.getId(), 3, 100);
        final Set<JobEntry> entries =
                mockDaoDriver.createJobWithEntries(
                        JobRequestType.PUT,
                        CollectionFactory.toSet( blobs.get(0), blobs.get(1), blobs.get(2)) );
        //final UUID jobId = mockDaoDriver.createJobWithEntry(
                //JobRequestType.GET, blobs.get(0) ).getJobId();

        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/job/" + entries.iterator().next().getJobId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( entries.iterator().next().getJobId().toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( JobEntry.class ).setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                JobEntry.BLOB_STORE_STATE );

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/job/" + entries.iterator().next().getJobId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( entries.iterator().next().getJobId().toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( JobEntry.class ).setBlobStoreState( JobChunkBlobStoreState.COMPLETED ),
                JobEntry.BLOB_STORE_STATE );
        final File file = cacheFilesystemDriver.writeCacheFile( blobs.get(0).getId(), 100 );
        file.deleteOnExit();
        final BlobCache bc = BeanFactory.newBean(BlobCache.class)
                .setBlobId(blobs.get(0).getId())
                .setState(CacheEntryState.IN_CACHE)
                .setLastAccessed(new Date(System.currentTimeMillis()))
                . setPath(file.getPath());

        dbSupport.getDataManager().createBean( bc );
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/job/" + entries.iterator().next().getJobId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( entries.iterator().next().getJobId().toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );

        final File file2 = cacheFilesystemDriver.writeCacheFile( blobs.get(1).getId(), 100 );
        file2.deleteOnExit();
        final BlobCache bc2 = BeanFactory.newBean(BlobCache.class)
                .setBlobId(blobs.get(1).getId())
                .setState(CacheEntryState.IN_CACHE)
                .setLastAccessed(new Date(System.currentTimeMillis()))
                . setPath(file2.getPath());

        dbSupport.getDataManager().createBean( bc2 );
        final File file3 = cacheFilesystemDriver.writeCacheFile( blobs.get(2).getId(), 100 );
        file3.deleteOnExit();
        final BlobCache bc3 = BeanFactory.newBean(BlobCache.class)
                .setBlobId(blobs.get(2).getId())
                .setState(CacheEntryState.IN_CACHE)
                .setLastAccessed(new Date(System.currentTimeMillis()))
                . setPath(file3.getPath());

        dbSupport.getDataManager().createBean( bc3 );
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/job/" + entries.iterator().next().getJobId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( entries.iterator().next().getJobId().toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"true\"" );
    }
    
    
    @Test
    public void testGetJobInfoReturnsEntirelyInCacheCorrectlyForPutJobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final UUID jobId = mockDaoDriver.createJobWithEntry(
                JobRequestType.PUT, b ).getJobId();

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        
        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( JobEntry.class ).setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                JobEntry.BLOB_STORE_STATE );

        final File file1 = cacheFilesystemDriver.writeCacheFile( b.getId(), 100 );
        file1.deleteOnExit();
        final BlobCache bc1 = BeanFactory.newBean(BlobCache.class)
                .setBlobId(b.getId())
                .setState(CacheEntryState.IN_CACHE)
                .setLastAccessed(new Date(System.currentTimeMillis()))
                . setPath(file1.getPath());

        dbSupport.getDataManager().createBean( bc1 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"true\"" );
        
        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( JobEntry.class ).setBlobStoreState( JobChunkBlobStoreState.COMPLETED ),
                JobEntry.BLOB_STORE_STATE );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"true\"" );
        
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( jobId,  b2 );

        mockDaoDriver.updateBean( 
                chunk2.setBlobStoreState( JobChunkBlobStoreState.PENDING ),
                JobEntry.BLOB_STORE_STATE );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        
        mockDaoDriver.updateBean( 
                chunk2.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                JobEntry.BLOB_STORE_STATE );
        final File file2 = cacheFilesystemDriver.writeCacheFile( b2.getId(), 100 );
        file2.deleteOnExit();
        final BlobCache bc2 = BeanFactory.newBean(BlobCache.class)
                .setBlobId(b2.getId())
                .setState(CacheEntryState.IN_CACHE)
                .setLastAccessed(new Date(System.currentTimeMillis()))
                . setPath(file2.getPath());

        dbSupport.getDataManager().createBean( bc2 );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( "EntirelyInCache=\"true\"" );
    }
    
    
    @Test
    public void testGetJobInfoForCanceledJobReturnsCorrectlyAndEntirelyInCacheNull()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final CanceledJob job = mockDaoDriver.createCanceledJob( null, null, null, null );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( "EntirelyInCache" );
        driver.assertResponseToClientContains( "CANCELED" );
    }
    
    
    @Test
    public void testGetJobInfoForCompletedJobReturnsCorrectlyAndEntirelyInCacheNull()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( "EntirelyInCache" );
        driver.assertResponseToClientContains( "COMPLETED" );
    }

    private static DatabaseSupport dbSupport;
    private static MockCacheFilesystemDriver cacheFilesystemDriver ;
    @BeforeAll
    public static void setDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
    }

    @BeforeEach
    public void setUp() {
        dbSupport.reset();
    }

}
