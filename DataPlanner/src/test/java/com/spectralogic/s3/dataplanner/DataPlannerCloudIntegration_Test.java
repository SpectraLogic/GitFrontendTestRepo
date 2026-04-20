package com.spectralogic.s3.dataplanner;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.CompletedJobService;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreateGetJobParams;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreatePutJobParams;
import com.spectralogic.s3.common.rpc.dataplanner.domain.S3ObjectToCreate;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.testfrmwrk.DataPlannerIntegrationTester;
import com.spectralogic.s3.dataplanner.testfrmwrk.ObjectsCachedListener;
import com.spectralogic.s3.simulator.Simulator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.TestPerfMonitor;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import java.net.URI;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Tag("dataplanner-integration")
public class DataPlannerCloudIntegration_Test {

    @Test
    @Tag( "public-cloud-integration" )
    /*
    For this test localstack docker container should be running.
     */
    public void testPutToAndGetS3() {
        final int numObjects = 10;
        try (final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig())) {
            tester.start();
            TestPerfMonitor.hit("Started test");
            forceDeleteBucket("testbucket");
            final BeansServiceManager serviceManager = tester.getServiceManager();
            final DataPlannerResource plannerResource = tester.getPlannerResource();
            final UUID adminId = tester.getAdminUser().getId();
            final MockDaoDriver mockDaoDriver = tester.getMockDaoDriver();
            final MockCacheFilesystemDriver cacheFilesystemDriver = tester.getCacheFilesystemDriver();

            tester.getTapeResource().forceTapeEnvironmentRefresh().get(RpcFuture.Timeout.DEFAULT);
            mockDaoDriver.unquiesceAllTapePartitions();
            mockDaoDriver.addS3FeatureKey();
            S3Target s3Target = createS3Target(mockDaoDriver);
            final DataPolicy dp = mockDaoDriver.createABMConfigTapeAndCloud(s3Target);
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

                // 2. Calculate the MD5 of 10 zero-bytes
                byte[] zeroBytes = new byte[10]; // Match the size used in writeCacheFile
                byte[] md5Hash = MessageDigest.getInstance("MD5").digest(zeroBytes);
                String base64Md5 = Base64.getEncoder().encodeToString(md5Hash);

                //plannerResource.startBlobWrite(jobId, blobId).get(RpcFuture.Timeout.DEFAULT);
                //cacheFilesystemDriver.writeCacheFile(blobId, 10);
                plannerResource.blobWriteCompleted(jobId, blobId, ChecksumType.MD5, base64Md5, null, new S3ObjectProperty[]{}).get(RpcFuture.Timeout.DEFAULT);
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

            final UUID getJobId = plannerResource.createGetJob(gParams).get(RpcFuture.Timeout.DEFAULT);
            for (final UUID blobId : blobIds) {
                JobEntry entry = mockDaoDriver.getJobEntryFor(blobId);
                mockDaoDriver.updateBean(
                        entry.setReadFromTapeId(null),
                        JobEntry.READ_FROM_TAPE_ID );
                mockDaoDriver.updateBean(
                        entry.setReadFromS3TargetId(s3Target.getId()),
                        JobEntry.READ_FROM_S3_TARGET_ID );
            }

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

            for (final UUID blobId : blobIds) {
                plannerResource.startBlobRead(getJobId, blobId).get(RpcFuture.Timeout.DEFAULT);
                plannerResource.blobReadCompleted(getJobId, blobId).get(RpcFuture.Timeout.DEFAULT);
            }

            TestPerfMonitor.hit( "Waiting for job to complete.");
            while(serviceManager.getService(JobService.class).any(Require.nothing())) {
                TestUtil.sleep(1000);
            }
            TestPerfMonitor.hit("Get Job complete");

            //assert that there is a completed job now
            assertEquals(2,  serviceManager.getService(CompletedJobService.class).getCount(), "There should be three completed jobs");
            assertEquals(1, notificationImpl.getJobIds().size(), "There should be notification for the GET job.");
            assertTrue(notificationImpl.getJobIds().contains(getJobId), "Notification is generated for the GET job.");

        } catch (final Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    public void forceDeleteBucket(String bucketName) {
        S3Client s3 = getLocalStackS3Client();

        try {
            s3.listObjectsV2Paginator(b -> b.bucket(bucketName)).contents().forEach(object -> {
                s3.deleteObject(d -> d.bucket(bucketName).key(object.key()));
            });

            s3.deleteBucket(b -> b.bucket(bucketName));
            System.out.println("Bucket deleted.");

        } catch (NoSuchBucketException e) {
            System.out.println("Bucket did not exist.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            s3.close();
        }
    }

    public S3Client getLocalStackS3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")
                ))
                .forcePathStyle(true)
                .build();
    }

    public S3Target createS3Target(MockDaoDriver mockDaoDriver ) {
        S3Target s3Target = mockDaoDriver.createS3TargetToAmazon(getClass().getSimpleName());
        s3Target.setAccessKey( S3_ACCESS_KEY );
        s3Target.setSecretKey( S3_SECRET_KEY );
        s3Target.setDataPathEndPoint(S3_ENDPOINT);
        s3Target.setRegion(S3Region.US_EAST_1);
        s3Target.setHttps(false);
        mockDaoDriver.updateBean( s3Target, S3Target.ACCESS_KEY, S3Target.SECRET_KEY, S3Target.DATA_PATH_END_POINT, S3Target.HTTPS );
        return s3Target;
    }

    public final String S3_ACCESS_KEY = "test";
    public final String S3_SECRET_KEY = "test";
    public final String S3_ENDPOINT = "s3.localhost.localstack.cloud:4566";
}
