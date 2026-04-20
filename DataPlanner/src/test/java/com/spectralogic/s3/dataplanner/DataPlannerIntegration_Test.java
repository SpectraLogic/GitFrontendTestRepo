package com.spectralogic.s3.dataplanner;

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
public final class DataPlannerIntegration_Test {

    @Test
    public void testPutJobToTape() {
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())) {
            tester.start();

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();

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
            long startTime = System.currentTimeMillis();
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                if (System.currentTimeMillis() - startTime > 5 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 5 minutes waiting for job to complete");
                }
                TestUtil.sleep(1000);
            }
            //assert that there is a completed job now
            assertEquals(1,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be one completed job");
            assertEquals(1,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be one blob tape");
        }
    }

    @Test
    public void testPutJobToDualCopyOnTape() {
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(
                Simulator.getTestConfig()
                        .setTapesPerPartition(2)
                        .setDrivesPerPartition(2))) {
            tester.start();

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            final DataPolicy dp = mockDaoDriver.createABMConfigDualCopyOnTape();

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
            assertEquals(2,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be two blob tapes");
        }
    }


    @Test
    public void testPutJobToDualCopyOnTapeOneRetired() {
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(
                Simulator.getTestConfig()
                        .setTapesPerPartition(2)
                        .setDrivesPerPartition(2))) {
            tester.start();

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            final DataPolicy dp = mockDaoDriver.createABMConfigDualCopyOnTape();

            final DataPersistenceRule ruleToRetire = mockDaoDriver.retrieveAll(DataPersistenceRule.class).iterator().next();
            mockDaoDriver.updateBean(ruleToRetire.setType(DataPersistenceRuleType.RETIRED), DataPersistenceRule.TYPE);


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
            assertEquals(1,  serviceManager.getRetriever(LocalBlobDestination.class).getCount(), "There should be two blob destinations");
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
            assertEquals(1,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be two blob tapes");
        }
    }


    @Test
    public void testPutJobToTapeResumesAfterLibraryDisconnect() {
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())) {
            tester.start();

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();

            final Bucket bucket = BeanFactory.newBean(Bucket.class);
            bucket.setName("testBucket")
                    .setDataPolicyId(dp.getId())
                    .setUserId(adminId)
                    .setId(UUID.randomUUID());
            tester.getPolicyResource().createBucket(bucket).get(RpcFuture.Timeout.DEFAULT);
            serviceManager.getService(BucketService.class).retrieveAll().getFirst().getName();
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
            System.out.println("Sleeping for 20 seconds, then killing");
            TestUtil.sleep(1000 * 20);
            final TapeEnvironmentResource goodResource = tester.getTapeBackendSimulator().getStateManager(10 * 1000).swapTapeEnvironmentResource(new ErrorSimTapeEnvironmentResource());
            final SimDevices goodDevices = tester.getTapeBackendSimulator().getStateManager(10 * 1000).swapSimDevices(new SimDevices());
            System.out.println("Leaving down for 2 minutes");
            TestUtil.sleep(1000 * 60 * 2);
            //restore good resources and devices
            System.out.println("Restoring");
            tester.getTapeBackendSimulator().getStateManager(10 * 1000).swapTapeEnvironmentResource(goodResource);
            tester.getTapeBackendSimulator().getStateManager(10 * 1000).swapSimDevices(goodDevices);
            mockDaoDriver.unquiesceAllTapePartitions();
            long startTime = System.currentTimeMillis();
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                if (System.currentTimeMillis() - startTime > 5 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 5 minutes waiting for job to complete after library disconnect restore");
                }
                TestUtil.sleep(1000);
            }
            //assert that there is a completed job now
            assertEquals(1,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be one completed job");
            assertEquals(1,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be one blob tape");
        }
    }


    @Test
    public void testPutToAndGetFromTapeWithSomeZeroByteBlobs() {
        final int numObjects = 100;
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())) {
            tester.start();
            TestPerfMonitor.hit("Started test");

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
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
                objectsToCreate[i] = BeanFactory.newBean(S3ObjectToCreate.class).setName("o" + (i + 1))
                        .setSizeInBytes(Math.max(0, i - numObjects / 10)); //This will give 10% of the objects size 0.
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
                cacheFilesystemDriver.writeCacheFile(blobId, mockDaoDriver.attain(Blob.class, blobId).getLength());
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
            assertEquals(numObjects,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be " + numObjects + " blob tapes");

            //verify there is only one blob cache entry:
            assertEquals(numObjects,  serviceManager.getService(BlobCacheService.class).getCount(), "There should be " + numObjects + " blob cache entries");
            plannerResource.forceFullCacheReclaimNow().get(RpcFuture.Timeout.DEFAULT);
            assertEquals(true, serviceManager.getService(BlobCacheService.class)
                    .all(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)), "Any blobs still in cache should be pending delete.");
            mockDaoDriver.quiesceAllTapePartitions();
            tester.getTapeResource().flagEnvironmentForRefresh().get(RpcFuture.Timeout.DEFAULT);
            do {
                //System.out.println(new Date() + "Waiting until tape partitions are quiesced.");
                TestUtil.sleep(100);
            } while (!serviceManager.getService(TapePartitionService.class)
                    .all(Require.beanPropertyEquals(TapePartition.QUIESCED, Quiesced.YES)));
            TestPerfMonitor.hit("Partitions quiesced");

            assertTrue(serviceManager.getService(TapeDriveService.class).all(Require.beanPropertyEquals(TapeDrive.TAPE_ID, null)), "No drives should have tapes in them currently");

            mockDaoDriver.unquiesceAllTapePartitions();

            final CreateGetJobParams gParams = BeanFactory.newBean(CreateGetJobParams.class)
                    .setUserId(adminId)
                    .setBlobIds(blobIds.toArray(new UUID[blobIds.size()]));
            final UUID getJobId = plannerResource.createGetJob(gParams).get(RpcFuture.Timeout.DEFAULT);
            TestPerfMonitor.hit("get job created");
            do {
                //System.out.println(new Date() + "Waiting for blob to be allocated.");
                TestUtil.sleep(1000);
            } while (mockDaoDriver.retrieveAll(BlobCache.class).isEmpty());

            //wait for blob to be in cache
            do {
                //System.out.println(new Date() + "Waiting for blob to be in cache.");
                TestUtil.sleep(100);
            } while (mockDaoDriver.retrieveAll(BlobCache.class).stream().anyMatch(b -> b.getState() != CacheEntryState.IN_CACHE));
            TestPerfMonitor.hit("Blobs in cache");

            for (final UUID blobId : blobIds) {
                assertEquals(CacheEntryState.IN_CACHE, serviceManager.getService(BlobCacheService.class).attain(Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId)).getState(), "Blob should be in cache now");
            }

            do {
                //System.out.println(new Date() + "Waiting for blob to be in cache.");
                TestUtil.sleep(100);
            } while (mockDaoDriver.retrieveAll(JobEntry.class).stream().anyMatch(b -> b.getBlobStoreState() != JobChunkBlobStoreState.COMPLETED));
            TestPerfMonitor.hit("All chunks complete");

            assertEquals(1,  serviceManager.getService(TapeDriveService.class).getCount(
                    Require.beanPropertyEquals(
                            TapeDrive.TAPE_ID,
                            mockDaoDriver.retrieveAll(BlobTape.class).stream().findFirst().get().getTapeId())), "One drive should have a tape in it currently");
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


    @Test
    public void testPutToAndGetFromTape() {
        final int numObjects = 100;
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())) {
            tester.start();
            TestPerfMonitor.hit("Started test");

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
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
            assertEquals(numObjects,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be " + numObjects + " blob tapes");

            //verify there is only one blob cache entry:
            assertEquals(numObjects,  serviceManager.getService(BlobCacheService.class).getCount(), "There should be " + numObjects + " blob cache entries");
            plannerResource.forceFullCacheReclaimNow().get(RpcFuture.Timeout.DEFAULT);
            assertEquals(true, serviceManager.getService(BlobCacheService.class)
                                .all(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)), "Any blobs still in cache should be pending delete.");
            mockDaoDriver.quiesceAllTapePartitions();
            tester.getTapeResource().flagEnvironmentForRefresh().get(RpcFuture.Timeout.DEFAULT);
            do {
                //System.out.println(new Date() + "Waiting until tape partitions are quiesced.");
                TestUtil.sleep(100);
            } while (!serviceManager.getService(TapePartitionService.class)
                    .all(Require.beanPropertyEquals(TapePartition.QUIESCED, Quiesced.YES)));
            TestPerfMonitor.hit("Partitions quiesced");

            assertTrue(serviceManager.getService(TapeDriveService.class).all(Require.beanPropertyEquals(TapeDrive.TAPE_ID, null)), "No drives should have tapes in them currently");

            mockDaoDriver.unquiesceAllTapePartitions();

            final CreateGetJobParams gParams = BeanFactory.newBean(CreateGetJobParams.class)
                    .setUserId(adminId)
                    .setBlobIds(blobIds.toArray(new UUID[blobIds.size()]));
            final UUID getJobId = plannerResource.createGetJob(gParams).get(RpcFuture.Timeout.DEFAULT);
            TestPerfMonitor.hit("get job created");
            do {
                //System.out.println(new Date() + "Waiting for blob to be allocated.");
                TestUtil.sleep(1000);
            } while (mockDaoDriver.retrieveAll(BlobCache.class).isEmpty());

            //wait for blob to be in cache
            do {
                //System.out.println(new Date() + "Waiting for blob to be in cache.");
                TestUtil.sleep(100);
            } while (mockDaoDriver.retrieveAll(BlobCache.class).stream().anyMatch(b -> b.getState() != CacheEntryState.IN_CACHE));
            TestPerfMonitor.hit("Blobs in cache");

            for (final UUID blobId : blobIds) {
                assertEquals(CacheEntryState.IN_CACHE, serviceManager.getService(BlobCacheService.class).attain(Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId)).getState(), "Blob should be in cache now");
            }

            do {
                //System.out.println(new Date() + "Waiting for blob to be in cache.");
                TestUtil.sleep(100);
            } while (mockDaoDriver.retrieveAll(JobEntry.class).stream().anyMatch(b -> b.getBlobStoreState() != JobChunkBlobStoreState.COMPLETED));
            TestPerfMonitor.hit("All chunks complete");

            assertEquals(1,  serviceManager.getService(TapeDriveService.class).getCount(
                    Require.beanPropertyEquals(
                            TapeDrive.TAPE_ID,
                            mockDaoDriver.retrieveAll(BlobTape.class).stream().findFirst().get().getTapeId())), "One drive should have a tape in it currently");
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

    @Test
    public void testPutToAndMultipleGetFromTape() {
        final int numObjects = 100;
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())) {
            tester.start();
            TestPerfMonitor.hit("Started test");

            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
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
            assertEquals(numObjects,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be " + numObjects + " blob tapes");

            //verify there is only one blob cache entry:
            assertEquals(numObjects,  serviceManager.getService(BlobCacheService.class).getCount(), "There should be " + numObjects + " blob cache entries");
            plannerResource.forceFullCacheReclaimNow().get(RpcFuture.Timeout.DEFAULT);
            assertEquals(true, serviceManager.getService(BlobCacheService.class)
                    .all(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)), "Any blobs still in cache should be pending delete.");
            mockDaoDriver.quiesceAllTapePartitions();
            tester.getTapeResource().flagEnvironmentForRefresh().get(RpcFuture.Timeout.DEFAULT);
            do {
                //System.out.println(new Date() + "Waiting until tape partitions are quiesced.");
                TestUtil.sleep(100);
            } while (!serviceManager.getService(TapePartitionService.class)
                    .all(Require.beanPropertyEquals(TapePartition.QUIESCED, Quiesced.YES)));
            TestPerfMonitor.hit("Partitions quiesced");

            assertTrue(serviceManager.getService(TapeDriveService.class).all(Require.beanPropertyEquals(TapeDrive.TAPE_ID, null)), "No drives should have tapes in them currently");

            mockDaoDriver.unquiesceAllTapePartitions();
            plannerResource.forceFullCacheReclaimNow();
            ObjectsCachedListener notificationImpl = new ObjectsCachedListener();
            tester.getDataplanner().getServiceManager().getNotificationEventDispatcher().registerListener(notificationImpl, S3ObjectCachedNotificationRegistration.class);
            final CreateGetJobParams gParams = BeanFactory.newBean(CreateGetJobParams.class)
                    .setUserId(adminId)
                    .setBlobIds(blobIds.toArray(new UUID[blobIds.size()]))
                    .setPriority(BlobStoreTaskPriority.CRITICAL);

            final CreateGetJobParams gParams2 = BeanFactory.newBean(CreateGetJobParams.class)
                    .setUserId(adminId)
                    .setBlobIds(blobIds.toArray(new UUID[blobIds.size()]))
                    .setPriority(BlobStoreTaskPriority.NORMAL);
            final UUID getJobId = plannerResource.createGetJob(gParams).get(RpcFuture.Timeout.DEFAULT);
            final UUID getJobId2 = plannerResource.createGetJob(gParams2).get(RpcFuture.Timeout.DEFAULT);
            TestPerfMonitor.hit("get jobs created: " + getJobId + " " + getJobId2);
            TestPerfMonitor.hit( "Waiting for blob to be allocated.");
            do {
                TestUtil.sleep(1000);
            } while (mockDaoDriver.retrieveAll(BlobCache.class).isEmpty());
            TestPerfMonitor.hit( "blob allocated.");
            TestPerfMonitor.hit( "Waiting for blob to be in cache.");
            do {
                TestUtil.sleep(100);
            } while (mockDaoDriver.retrieveAll(BlobCache.class).stream().anyMatch(b -> b.getState() != CacheEntryState.IN_CACHE));
            TestPerfMonitor.hit("Blobs in cache");

            for (final UUID blobId : blobIds) {
                assertEquals(CacheEntryState.IN_CACHE, serviceManager.getService(BlobCacheService.class).attain(Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId)).getState(), "Blob should be in cache now");
            }

            TestPerfMonitor.hit("Waiting for blob to be in cache.");
            do {
                TestUtil.sleep(100);
            } while (mockDaoDriver.retrieveAll(JobEntry.class).stream().anyMatch(b -> b.getBlobStoreState() != JobChunkBlobStoreState.COMPLETED));
            TestPerfMonitor.hit("All chunks complete");

            assertEquals(1,  serviceManager.getService(TapeDriveService.class).getCount(
                    Require.beanPropertyEquals(
                            TapeDrive.TAPE_ID,
                            mockDaoDriver.retrieveAll(BlobTape.class).stream().findFirst().get().getTapeId())), "One drive should have a tape in it currently");
            for (final UUID blobId : blobIds) {
                plannerResource.startBlobRead(getJobId, blobId).get(RpcFuture.Timeout.DEFAULT);
                plannerResource.blobReadCompleted(getJobId, blobId).get(RpcFuture.Timeout.DEFAULT);

                plannerResource.startBlobRead(getJobId2, blobId).get(RpcFuture.Timeout.DEFAULT);
                plannerResource.blobReadCompleted(getJobId2, blobId).get(RpcFuture.Timeout.DEFAULT);

            }

            TestPerfMonitor.hit( "Waiting for job to complete.");
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                TestUtil.sleep(1000);
            }
            TestPerfMonitor.hit("Get Job complete");

            //assert that there is a completed job now
            assertEquals(3,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be three completed jobs");
            assertEquals(2, notificationImpl.getJobIds().size(), "There should be 2 notifications for GET jobs.");
            assertTrue(notificationImpl.getJobIds().contains(getJobId), "Notification is generated for first GET job.");
            assertTrue(notificationImpl.getJobIds().contains(getJobId2), "Notification is generated for second GET job.");
        } catch (final Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /*
     Test to verify drive is not quiesced if there are tapes with single partitions
     */
    @Test
    public void testMemoryPartitionFailure() {
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())){
            tester.start();
            final BeansServiceManager serviceManager = tester.getServiceManager();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();

            UUID id = serviceManager.getService(TapeService.class).attain(Require.nothing()).getId();
            TapeManagementResource res = tester.getTapeResource();
            DataPlannerResource dataPlannerResource = tester.getPlannerResource();
            res.inspectTape(id, BlobStoreTaskPriority.LOW);
            BlobStoreTaskState [] taskStates = new BlobStoreTaskState [] {BlobStoreTaskState.IN_PROGRESS, BlobStoreTaskState.PENDING_EXECUTION, BlobStoreTaskState.READY};
            BlobStoreTaskState [] completed = new BlobStoreTaskState [] {BlobStoreTaskState.COMPLETED};
            TestUtil.sleep(1000);

            long startTime = System.currentTimeMillis();
            while(dataPlannerResource.getBlobStoreTasks(taskStates).get(RpcFuture.Timeout.DEFAULT).getTasks().length != 0 &&
                    dataPlannerResource.getBlobStoreTasks(completed).get(RpcFuture.Timeout.DEFAULT).getTasks().length ==0) {
                if (System.currentTimeMillis() - startTime > 5 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 5 minutes waiting for inspect task to complete");
                }
                System.out.println("Waiting for inspect task to be complete.");
                TestUtil.sleep(1000);
            }
            TapeDrive drive = serviceManager.getService(TapeDriveService.class).attain(Require.nothing());
            assertEquals(Quiesced.NO, drive.getQuiesced(), "Drive should not have been quiesced");
            TapeState state = serviceManager.getService(TapeService.class).attain(Require.nothing()).getState();
            assertEquals(NORMAL, state, "Tape should not have been quiesced");

        } catch (final Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    @Test
    public void testGetJobTriesSecondCopyIfFirstFails() {
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig().setTapesPerPartition(2).setDrivesPerPartition(2))) {
            tester.start();
            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            mockDaoDriver.updateBean(mockDaoDriver.attainOneAndOnly(DataPathBackend.class).setIomEnabled(false), DataPathBackend.IOM_ENABLED);

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            final DataPolicy dp = mockDaoDriver.createABMConfigDualCopyOnTape();

            final Bucket bucket = BeanFactory.newBean(Bucket.class);
            bucket.setName("testBucket")
                    .setDataPolicyId(dp.getId())
                    .setUserId(adminId)
                    .setId(UUID.randomUUID());
            tester.getPolicyResource().createBucket(bucket).get(RpcFuture.Timeout.DEFAULT);
            serviceManager.getService(BucketService.class).retrieveAll().getFirst().getName();
            final CreatePutJobParams params = BeanFactory.newBean(CreatePutJobParams.class)
                    .setBucketId(bucket.getId())
                    .setUserId(adminId)
                    .setObjectsToCreate(new S3ObjectToCreate[] {
                            BeanFactory.newBean(S3ObjectToCreate.class).setName("o1").setSizeInBytes(10)});
            final UUID jobId = plannerResource.createPutJob(params).get(RpcFuture.Timeout.DEFAULT);
            final UUID objectId1 = serviceManager.getService(S3ObjectService.class).attain(Require.beanPropertyEquals(S3Object.NAME, "o1")).getId();
            final UUID blobId1 = mockDaoDriver.getBlobFor(objectId1).getId();
            final UUID chunkId1 = mockDaoDriver.attainOneAndOnly(JobEntry.class).getId();
            plannerResource.allocateEntry( chunkId1 ).get( RpcFuture.Timeout.LONG );
            BlobCache bc1 = serviceManager.getService(BlobCacheService.class).attain(Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId1));

            Object x3 = bc1.getState();
            assertEquals(x3, CacheEntryState.ALLOCATED, "Blob should be allocated, but not yet in cache");
            plannerResource.startBlobWrite(jobId, blobId1).get(RpcFuture.Timeout.DEFAULT);
            cacheFilesystemDriver.writeCacheFile(blobId1, 10);
            plannerResource.blobWriteCompleted(jobId, blobId1, ChecksumType.MD5, "TEST_CHECKSUM", null, new S3ObjectProperty[]{}).get(RpcFuture.Timeout.DEFAULT);
            bc1 = serviceManager.getService(BlobCacheService.class).attain(Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId1));
            Object x1 = bc1.getState();
            assertEquals(x1, CacheEntryState.IN_CACHE, "Blob 1 should be in cache now");
            long startTime = System.currentTimeMillis();
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                if (System.currentTimeMillis() - startTime > 5 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 5 minutes waiting for dual copy job to complete");
                }
                System.out.println("Waiting for job to complete.");
                TestUtil.sleep(1000);
            }
            //assert that there is a completed job now
            assertEquals(1,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be one completed job");
            assertEquals(2,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be four blobs on tape");


            //verify there are only two blob cache entries:
            assertEquals(1,  serviceManager.getService(BlobCacheService.class).getCount(), "There should be two blobs in cache");
            plannerResource.forceFullCacheReclaimNow().get(RpcFuture.Timeout.DEFAULT);
            //wait until no blobs are pending delete
            while (serviceManager.getService(BlobCacheService.class).any(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE))) {
                System.out.println("Waiting for blobs to be deleted from cache.");
                TestUtil.sleep(500);
            }
            assertEquals(0,  serviceManager.getService(BlobCacheService.class).getCount(), "There should be no blobs in cache");
            mockDaoDriver.quiesceAllTapePartitions();
            startTime = System.currentTimeMillis();
            do {
                if (System.currentTimeMillis() - startTime > 5 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 5 minutes waiting for tape partitions to be quiesced");
                }
                System.out.println("Waiting until tape partitions are quiesced.");
                TestUtil.sleep(1000);
            } while (!serviceManager.getService(TapePartitionService.class)
                    .all(Require.beanPropertyEquals(TapePartition.QUIESCED, Quiesced.YES)));

            assertTrue(
                    serviceManager.getService(TapeDriveService.class).all(Require.beanPropertyEquals(TapeDrive.TAPE_ID, null)),
                    "No drives should have tapes in them currently");

            mockDaoDriver.unquiesceAllTapePartitions();

            final List<TapeDrive> drives = mockDaoDriver.retrieveAll(TapeDrive.class).stream().toList();
            final TapeDrive drive1 = drives.get(0);
            final TapeDrive drive2 = drives.get(1);
            System.out.println("Swapping drive resource");
            final TapeDriveResource goodResource1 = tester.getTapeBackendSimulator().getStateManager(10)
                    .getTapeDriveResource(drive1.getSerialNumber());
            final TapeDriveResource goodResource2 = tester.getTapeBackendSimulator().getStateManager(10)
                    .getTapeDriveResource(drive2.getSerialNumber());
            final TapeDriveResource readFailingResource1 = Mockito.spy(goodResource1);
            final TapeDriveResource readFailingResource2 = Mockito.spy(goodResource2);
            final BlobIoFailure failure1 = BeanFactory.newBean(BlobIoFailure.class)
                    .setBlobId(blobId1)
                    .setFailure(BlobIoFailureType.DOES_NOT_EXIST);
            final BlobIoFailures readFailures = BeanFactory.newBean(BlobIoFailures.class)
                    .setFailures(new BlobIoFailure[]{failure1});
            doAnswer( invocation -> {
                System.out.println("Read failure hit on drive 1");
                return new RpcResponse<>(readFailures);
            }).when(readFailingResource1).readData(any());
            doAnswer( invocation -> {
                System.out.println("Read failure hit on drive 2");
                return new RpcResponse<>(readFailures);
            }).when(readFailingResource2).readData(any());
            tester.getTapeBackendSimulator().getStateManager(10).swapTapeDriveResource(
                    drive1.getSerialNumber(),
                    readFailingResource1);
            tester.getTapeBackendSimulator().getStateManager(10).swapTapeDriveResource(
                    drive2.getSerialNumber(),
                    readFailingResource2);


            final CreateGetJobParams gParams = BeanFactory.newBean(CreateGetJobParams.class)
                    .setUserId(adminId)
                    .setBlobIds(new UUID[] { blobId1 });
            plannerResource.createGetJob(gParams).get(RpcFuture.Timeout.DEFAULT);
            startTime = System.currentTimeMillis();
            do {
                if (System.currentTimeMillis() - startTime > 5 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 5 minutes waiting for blob to be allocated");
                }
                System.out.println("Waiting for blob to be allocated.");
                TestUtil.sleep(1000);
            } while (mockDaoDriver.retrieveAll(BlobCache.class).isEmpty());

            startTime = System.currentTimeMillis();
            do {
                if (System.currentTimeMillis() - startTime > 15 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 15 minutes waiting for suspect blobs to register");
                }
                System.out.println("Waiting for suspect blobs to register.");
                TestUtil.sleep(1000);
            } while (mockDaoDriver.retrieveAll(SuspectBlobTape.class).size() < 1);
            tester.getTapeBackendSimulator().getStateManager(10).swapTapeDriveResource(
                    drive1.getSerialNumber(),
                    goodResource1);
            tester.getTapeBackendSimulator().getStateManager(10).swapTapeDriveResource(
                    drive2.getSerialNumber(),
                    goodResource2);

            //wait for blob to be in cache
            startTime = System.currentTimeMillis();
            do {
                if (System.currentTimeMillis() - startTime > 5 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 5 minutes waiting for blob to be in cache");
                }
                System.out.println("Waiting for blob to be in cache.");
                TestUtil.sleep(1000);
            } while (mockDaoDriver.retrieveAll(BlobCache.class).stream().findFirst().get().getState() != CacheEntryState.IN_CACHE);

            assertEquals(CacheEntryState.IN_CACHE, serviceManager.getService(BlobCacheService.class).attain(Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId1)).getState(), "Blob 1 should be in cache now");
            assertEquals(1,  serviceManager.getService(TapeDriveService.class).getCount(
                    Require.beanPropertyEquals(
                            TapeDrive.TAPE_ID,
                            mockDaoDriver.retrieveAll(BlobTape.class).stream().findFirst().get().getTapeId())), "One drive should have a tape in it currently");
        } catch (final Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

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

    @Test
    public void testPutJobOneCopyPoolOneCopyTape() {
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
            assertNotEquals(
                    0,
                    tester.getPoolEnvResource().getPoolEnvironment().get(RpcFuture.Timeout.DEFAULT).getPools().length,
                    "Should have had simulated pools");
            tester.getPoolManagementResource().forcePoolEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            assertNotEquals(
                    0,
                    tester.getServiceManager().getRetriever(Pool.class).getCount(),
                    "Should have discovered pools");
            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();

            final DataPolicy dp = mockDaoDriver.createABMConfigOneCopyPoolOneCopyTape();

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
            assertEquals(2,  serviceManager.getRetriever(LocalBlobDestination.class).getCount(), "There should be two blob destinations (one pool, one tape)");
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
            startTime = System.currentTimeMillis();
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                if (System.currentTimeMillis() - startTime > 10 * 60 * 1000) {
                    throw new RuntimeException("Timed out after 10 minutes waiting for job to complete");
                }
                TestUtil.sleep(1000);
            }
            //assert that there is a completed job now
            assertEquals(1,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be one completed job");
            assertEquals(1,  serviceManager.getService(BlobPoolService.class).getCount(), "There should be one blob pool");
            assertEquals(1,  serviceManager.getService(BlobTapeService.class).getCount(), "There should be one blob tape");
        }
    }
}
