package com.spectralogic.s3.dataplanner.integrationtest;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.*;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.testfrmwrk.DataPlannerIntegrationTester;
import com.spectralogic.s3.dataplanner.testfrmwrk.ObjectsCachedListener;
import com.spectralogic.s3.simulator.Simulator;
import com.spectralogic.s3.simulator.state.simresource.SimDevices;
import com.spectralogic.s3.simulator.taperesource.ErrorSimTapeEnvironmentResource;


import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.TestPerfMonitor;
import com.spectralogic.util.testfrmwrk.TestUtil;


import org.junit.jupiter.api.Tag;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;


import java.util.*;

import static com.spectralogic.s3.common.dao.domain.tape.TapeState.NORMAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * NOTE: use the following JVM argument to get logging when running this test:
 * -Dlog4j.configuration=<< your workspace directory here >>/product/frontend/DataPlanner/src/test/resources/log4j.xml
 * For this to work inside IntelliJ you must use IntelliJ as the test runner, not gradle.
 */

//TODO: These tests are failing on the build server, but work locally.
// For now they are disabled in build server builds using SKIP_RPC_TESTS env variable.

@Tag("dataplanner-integration")
@Timeout(5 * 60 * 60)
public final class DataPlannerPoolIntegration_Test {

    @Test
    public void testPutJobToPool() {
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())) {
            tester.start();

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            //wait for pools to appear
            long startTime = System.currentTimeMillis();
            assertNotEquals(
                    0,
                    tester.getPoolEnvResource().getPoolEnvironment().get(RpcFuture.Timeout.DEFAULT).getPools().length,
                    "Should have had simulated pools");
            tester.getPoolManagementResource().forcePoolEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            assertNotEquals(
                    0,
                    tester.getServiceManager().getRetriever(Pool.class).getCount(),
                    "Should have discovered pools");

            final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnPool();

            final Bucket bucket = BeanFactory.newBean(Bucket.class);
            bucket.setName("testBucket")
                    .setDataPolicyId(dp.getId())
                    .setUserId(adminId)
                    .setId(UUID.randomUUID());
            tester.getPolicyResource().createBucket(bucket).get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.attainOneAndOnly(Bucket.class); //assure we have created the bucket
            final CreatePutJobParams params = BeanFactory.newBean(CreatePutJobParams.class)
                    .setBucketId(bucket.getId())
                    .setUserId(adminId)
                    .setObjectsToCreate(new S3ObjectToCreate[] { BeanFactory.newBean(S3ObjectToCreate.class)
                            .setName("o1").setSizeInBytes(10) });
            final UUID jobId = plannerResource.createPutJob(params).get(RpcFuture.Timeout.DEFAULT);
            final UUID blobId = serviceManager.getService(BlobService.class).attain(Require.nothing()).getId();
            final UUID chunkId = mockDaoDriver.attainOneAndOnly(JobEntry.class).getId();
            plannerResource.allocateEntry( chunkId ).get( RpcFuture.Timeout.LONG );
            BlobCache bc = mockDaoDriver.attainOneAndOnly(BlobCache.class);
            assertEquals(CacheEntryState.ALLOCATED, bc.getState(), "Blob should be allocated, but not yet in cache");
            plannerResource.startBlobWrite(jobId, blobId).get(RpcFuture.Timeout.DEFAULT);
            cacheFilesystemDriver.writeCacheFile(blobId, 10);
            plannerResource.blobWriteCompleted(jobId, blobId, ChecksumType.MD5, "TEST_CHECKSUM", null, new S3ObjectProperty[]{}).get(RpcFuture.Timeout.DEFAULT);
            bc = mockDaoDriver.attainOneAndOnly(BlobCache.class);
            assertEquals(CacheEntryState.IN_CACHE, bc.getState(), "Blob should be in cache now");
            startTime = System.currentTimeMillis();
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                if (System.currentTimeMillis() - startTime > 5 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 5 minutes waiting for job to complete");
                }
                TestUtil.sleep(1000);
            }
            //assert that there is a completed job now
            assertEquals(1,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be one completed job");
            assertEquals(1,  serviceManager.getService(BlobPoolService.class).getCount(), "There should be one blob pool");
        }
    }

    @Test
    public void testPutJobToDualCopyOnPool() {
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(
                Simulator.getTestConfig())) {
            tester.start();

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            //wait for pools to appear
            long startTime = System.currentTimeMillis();
            assertTrue(
                    tester.getPoolEnvResource().getPoolEnvironment().get(RpcFuture.Timeout.DEFAULT).getPools().length >= 2,
                    "Should have had simulated at least 2 pools");
            tester.getPoolManagementResource().forcePoolEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            assertTrue(
                    tester.getServiceManager().getRetriever(Pool.class).getCount() >= 2,
                    "Should have discovered pools");

            final DataPolicy dp = mockDaoDriver.createABMConfigDualCopyOnPool();

            final Bucket bucket = BeanFactory.newBean(Bucket.class);
            bucket.setName("testBucket")
                    .setDataPolicyId(dp.getId())
                    .setUserId(adminId)
                    .setId(UUID.randomUUID());
            tester.getPolicyResource().createBucket(bucket).get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.attainOneAndOnly(Bucket.class); //assure we have created the bucket
            final CreatePutJobParams params = BeanFactory.newBean(CreatePutJobParams.class)
                    .setBucketId(bucket.getId())
                    .setUserId(adminId)
                    .setObjectsToCreate(new S3ObjectToCreate[] { BeanFactory.newBean(S3ObjectToCreate.class)
                            .setName("o1").setSizeInBytes(10) });
            final UUID jobId = plannerResource.createPutJob(params).get(RpcFuture.Timeout.DEFAULT);
            assertEquals(2,  serviceManager.getRetriever(LocalBlobDestination.class).getCount(), "There should be two blob destinations");
            final UUID blobId = serviceManager.getService(BlobService.class).attain(Require.nothing()).getId();
            final UUID chunkId = mockDaoDriver.attainOneAndOnly(JobEntry.class).getId();
            plannerResource.allocateEntry( chunkId ).get( RpcFuture.Timeout.LONG );
            BlobCache bc = mockDaoDriver.attainOneAndOnly(BlobCache.class);
            final Object expected1 = bc.getState();
            assertEquals(expected1, CacheEntryState.ALLOCATED, "Blob should be allocated, but not yet in cache");
            plannerResource.startBlobWrite(jobId, blobId).get(RpcFuture.Timeout.DEFAULT);
            cacheFilesystemDriver.writeCacheFile(blobId, 10);
            plannerResource.blobWriteCompleted(jobId, blobId, ChecksumType.MD5, "TEST_CHECKSUM", null, new S3ObjectProperty[]{}).get(RpcFuture.Timeout.DEFAULT);
            bc = mockDaoDriver.attainOneAndOnly(BlobCache.class);
            final Object expected = bc.getState();
            assertEquals(expected, CacheEntryState.IN_CACHE, "Blob should be in cache now");
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                TestUtil.sleep(1000);
            }
            //assert that there is a completed job now
            assertEquals(1,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be one completed job");
            assertEquals(0,  serviceManager.getRetriever(LocalBlobDestination.class).getCount(), "There should be zero remaining blob destinations");
            assertEquals(2,  serviceManager.getService(BlobPoolService.class).getCount(), "There should be two blob pools");
        }
    }

    @Test
    public void testPutToAndGetFromPool() {
        final int numObjects = 100;
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())) {
            tester.start();
            TestPerfMonitor.hit("Started test");

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            //wait for pools to appear
            long startTime = System.currentTimeMillis();
            assertNotEquals(
                    0,
                    tester.getPoolEnvResource().getPoolEnvironment().get(RpcFuture.Timeout.DEFAULT).getPools().length,
                    "Should have had simulated pools");
            tester.getPoolManagementResource().forcePoolEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            assertNotEquals(
                    0,
                    tester.getServiceManager().getRetriever(Pool.class).getCount(),
                    "Should have discovered pools");

            final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnPool();
            TestPerfMonitor.hit("ABM set up");

            final Bucket bucket = BeanFactory.newBean(Bucket.class);
            bucket.setName("testBucket")
                    .setDataPolicyId(dp.getId())
                    .setUserId(adminId)
                    .setId(UUID.randomUUID());
            tester.getPolicyResource().createBucket(bucket).get(RpcFuture.Timeout.DEFAULT);
            serviceManager.getService(BucketService.class).retrieveAll().getFirst().getName();
            final S3ObjectToCreate[] objectsToCreate = new S3ObjectToCreate[numObjects];
            for (int i = 0; i < numObjects; i++) {
                objectsToCreate[i] = BeanFactory.newBean(S3ObjectToCreate.class).setName("o" + (i + 1)).setSizeInBytes(10);
            }
            final CreatePutJobParams params = BeanFactory.newBean(CreatePutJobParams.class)
                    .setBucketId(bucket.getId())
                    .setUserId(adminId)
                    .setObjectsToCreate(objectsToCreate);
            final UUID jobId = plannerResource.createPutJob(params).get(RpcFuture.Timeout.DEFAULT);
            TestPerfMonitor.hit("Created Put Job");
            final Set< UUID > objectIds = new HashSet<>();
            final Set< UUID > blobIds = new HashSet<>();
            final Set< UUID > chunkIds = new HashSet<>();
            for (int i = 0; i < numObjects; i++) {
                final UUID oid = serviceManager.getService(S3ObjectService.class).attain(Require.beanPropertyEquals(S3Object.NAME, "o" + (i + 1))).getId();
                objectIds.add(oid);
                final UUID blobId = mockDaoDriver.getBlobFor(oid).getId();
                blobIds.add(blobId);
                final UUID chunkId = mockDaoDriver.getJobEntryFor(blobId).getId();
                chunkIds.add(chunkId);
            }
            for (final UUID chunkId : chunkIds) {
                plannerResource.allocateEntry( chunkId ).get( RpcFuture.Timeout.LONG );
            }
            for (final UUID blobId : blobIds) {
                BlobCache bc = serviceManager.getService(BlobCacheService.class).attain(Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId));
                final Object expected = bc.getState();
                assertEquals(expected, CacheEntryState.ALLOCATED, "Blob should be allocated, but not yet in cache");
            }

            TestPerfMonitor.hit("Blobs Allocated");

            for (final UUID blobId : blobIds) {
                plannerResource.startBlobWrite(jobId, blobId).get(RpcFuture.Timeout.DEFAULT);
                cacheFilesystemDriver.writeCacheFile(blobId, 10);
                plannerResource.blobWriteCompleted(jobId, blobId, ChecksumType.MD5, "TEST_CHECKSUM", null, new S3ObjectProperty[]{}).get(RpcFuture.Timeout.DEFAULT);
            }
            for (final UUID blobId : blobIds) {
                BlobCache bc = serviceManager.getService(BlobCacheService.class).attain(Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId));
                final Object expected = bc.getState();
                assertEquals(expected, CacheEntryState.IN_CACHE, "Blob should be in cache now");
            }
            TestPerfMonitor.hit("Blobs in Cache");
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                //System.out.println(new Date() + "Waiting for job to complete.");
                TestUtil.sleep(20);
            }
            TestPerfMonitor.hit("Job Complete");
            //assert that there is a completed job now
            assertEquals(1,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be one completed job");
            assertEquals(numObjects,  serviceManager.getService(BlobPoolService.class).getCount(), "There should be " + numObjects + " blob pools");

            //verify there is only one blob cache entry:
            assertEquals(numObjects,  serviceManager.getService(BlobCacheService.class).getCount(), "There should be " + numObjects + " blob cache entries");
            plannerResource.forceFullCacheReclaimNow().get(RpcFuture.Timeout.DEFAULT);
            assertEquals(true, serviceManager.getService(BlobCacheService.class)
                                .all(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)), "Any blobs still in cache should be pending delete.");
            TestPerfMonitor.hit("Cache reclaimed");

            final CreateGetJobParams gParams = BeanFactory.newBean(CreateGetJobParams.class)
                    .setUserId(adminId)
                    .setBlobIds(blobIds.toArray(new UUID[blobIds.size()]));
            final UUID getJobId = plannerResource.createGetJob(gParams).get(RpcFuture.Timeout.DEFAULT);
            TestPerfMonitor.hit("get job created");

            //NOTE: we do not expect blobs to go to cache because they are going to be read directly from pool

            do {
                //System.out.println(new Date() + "Waiting for blob to be in cache.");
                TestUtil.sleep(100);
            } while (mockDaoDriver.retrieveAll(JobEntry.class).stream().anyMatch(b -> b.getBlobStoreState() != JobChunkBlobStoreState.COMPLETED));
            TestPerfMonitor.hit("All chunks complete");

            for (final UUID blobId : blobIds) {
                plannerResource.startBlobRead(getJobId, blobId).get(RpcFuture.Timeout.DEFAULT);
                plannerResource.blobReadCompleted(getJobId, blobId).get(RpcFuture.Timeout.DEFAULT);
            }
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                //System.out.println(new Date() + "Waiting for job to complete.");
                TestUtil.sleep(100);
            }
            TestPerfMonitor.hit("Get Job complete");
            //assert that there is a completed job now
            assertEquals(2,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be two completed jobs");
        } catch (final Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
