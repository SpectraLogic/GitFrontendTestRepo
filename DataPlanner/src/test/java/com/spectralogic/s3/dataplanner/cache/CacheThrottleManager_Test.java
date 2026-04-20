package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class CacheThrottleManager_Test {

    @Test
    public void getCacheThrottleRuleUsedCapacityInBytes_Test() {
        final DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final Bucket b1 = mockDaoDriver.createBucket(null, "a");
        final Bucket b2 = mockDaoDriver.createBucket(null, "b");

        final CacheThrottleManager ruleUsage = new CacheThrottleManager(dbSupport.getServiceManager());

        // Create GET jobs for Bucket1 of varying priorities
        final long b1LowGetBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b1.getId(), JobRequestType.GET, BlobStoreTaskPriority.LOW);
        final long b1NormalGetBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b1.getId(), JobRequestType.GET, BlobStoreTaskPriority.NORMAL);
        final long b1HighGetBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b1.getId(), JobRequestType.GET, BlobStoreTaskPriority.HIGH);

        // Create PUT jobs for Bucket1 of varying priorities
        final long b1LowPutBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b1.getId(), JobRequestType.PUT, BlobStoreTaskPriority.LOW);
        final long b1NormalPutBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b1.getId(), JobRequestType.PUT, BlobStoreTaskPriority.NORMAL);
        final long b1HighPutBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b1.getId(), JobRequestType.PUT, BlobStoreTaskPriority.HIGH);

        // Create GET jobs for Bucket2 of varying priorities
        final long b2LowGetBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b2.getId(), JobRequestType.GET, BlobStoreTaskPriority.LOW);
        final long b2NormalGetBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b2.getId(), JobRequestType.GET, BlobStoreTaskPriority.NORMAL);
        final long b2HighGetBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b2.getId(), JobRequestType.GET, BlobStoreTaskPriority.HIGH);

        // Create GET jobs for Bucket2 of varying priorities
        final long b2LowPutBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b2.getId(), JobRequestType.PUT, BlobStoreTaskPriority.LOW);
        final long b2NormalPutBytes = createJobForUsedCapacityInBytesTest(mockDaoDriver, b2.getId(), JobRequestType.PUT, BlobStoreTaskPriority.NORMAL);
        createJobForUsedCapacityInBytesTest(mockDaoDriver, b2.getId(), JobRequestType.PUT, BlobStoreTaskPriority.HIGH);

        final CacheThrottleRule ruleBucket1 = BeanFactory.newBean(CacheThrottleRule.class).setBucketId(b1.getId());
        final CacheThrottleRule rulePriorityNormal = BeanFactory.newBean(CacheThrottleRule.class).setPriority(BlobStoreTaskPriority.NORMAL);
        final CacheThrottleRule ruleRequestTypeGet = BeanFactory.newBean(CacheThrottleRule.class).setRequestType(JobRequestType.GET);
        final CacheThrottleRule ruleBucket1AndGet = BeanFactory.newBean(CacheThrottleRule.class).setBucketId(b1.getId()).setRequestType(JobRequestType.GET);
        final CacheThrottleRule ruleBucket1PriorityNormalRequestTypeGet = BeanFactory.newBean(CacheThrottleRule.class).setBucketId(b1.getId()).setPriority(BlobStoreTaskPriority.NORMAL).setRequestType(JobRequestType.GET);

        assertEquals(b1LowGetBytes+b1NormalGetBytes+b1HighGetBytes+b1LowPutBytes+b1NormalPutBytes+b1HighPutBytes, ruleUsage.getCacheThrottleRuleUsedCapacityInBytes(ruleBucket1), "expected number of bytes");
        assertEquals(b1LowGetBytes+b1NormalGetBytes+b1LowPutBytes+b1NormalPutBytes+b2LowGetBytes+b2NormalGetBytes+b2LowPutBytes+b2NormalPutBytes, ruleUsage.getCacheThrottleRuleUsedCapacityInBytes(rulePriorityNormal), "expected number of bytes");
        assertEquals(b1LowGetBytes+b1NormalGetBytes+b1HighGetBytes+b2LowGetBytes+b2NormalGetBytes+b2HighGetBytes, ruleUsage.getCacheThrottleRuleUsedCapacityInBytes(ruleRequestTypeGet), "expected number of bytes");
        assertEquals(b1LowGetBytes+b1NormalGetBytes+b1HighGetBytes, ruleUsage.getCacheThrottleRuleUsedCapacityInBytes(ruleBucket1AndGet), "expected number of bytes");
        assertEquals(b1LowGetBytes+b1NormalGetBytes, ruleUsage.getCacheThrottleRuleUsedCapacityInBytes(ruleBucket1PriorityNormalRequestTypeGet), "expected number of bytes");
    }

    private long createJobForUsedCapacityInBytesTest(final MockDaoDriver mockDaoDriver, final UUID bucketId, final JobRequestType requestType, final BlobStoreTaskPriority priority) {
        final int numObjects = 10;
        Random random = new Random();
        long totalSize = 0;

        final Job job = mockDaoDriver.createJob(bucketId, null, requestType);
        for (int i = 0; i < numObjects; i++) {
            final int objSize = random.nextInt(1, 10);
            final S3Object obj = mockDaoDriver.createObject(bucketId, "o" + i + "-" + job.getId(), objSize);
            final Blob blob = mockDaoDriver.getBlobFor(obj.getId());
            mockDaoDriver.createJobEntry(job.getId(), blob);
            final BlobCache blobCache = mockDaoDriver.markBlobInCache(blob.getId());
            totalSize += blobCache.getSizeInBytes();
        }
        mockDaoDriver.updateBean(job.setPriority(priority), Job.PRIORITY);
        return totalSize;
    }

    // Note: the delete count is retrieved from the postgres n_tup_del row from the pg_stat_all_tables table.
    // This value is updated periodically. If this test has periodic failures, then up the number of entries
    // being tested or comment out the test.
    @Test
    public void getNumJobEntryRowDeletes_Test() {
        final long totalCacheSize = 10000;
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver(dbSupport, totalCacheSize);
        try {
            final CacheThrottleManager ruleUsage = new CacheThrottleManager(dbSupport.getServiceManager());
            final long startRowDeleteCount = ruleUsage.getNumJobEntryRowDeletes();
            LOG.error("before: " + " del: " + ruleUsage.getNumJobEntryRowDeletes());

            final int count = 100;
            for (int i = 0; i < count; i++) {
                final S3Object o = mockDaoDriver.createObject( null, "o-" + i );
                final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
                final JobEntry entry = mockDaoDriver.createJobWithEntry(blob);

                // delete the job to verify cascade delete updates the delete row count
                mockDaoDriver.delete(JobEntry.class, entry);
            }

            final long endRowDeleteCount = ruleUsage.getNumJobEntryRowDeletes();
            assertNotEquals(startRowDeleteCount, endRowDeleteCount, "Expected delete count to change after deletes: start=" + startRowDeleteCount + " end=" + endRowDeleteCount);
        } finally {
            cacheFilesystemDriver.shutdown();
        }
    }

    @Test
    public void getApplicableCacheThrottleRules_Test() {
        final Bucket b1 = mockDaoDriver.createBucket(null, "a");
        final Bucket b2 = mockDaoDriver.createBucket(null, "b");

        final CacheThrottleManager throttleManager = new CacheThrottleManager(dbSupport.getServiceManager());

        assertTrue(throttleManager.getApplicableCacheThrottleRules(JobRequestType.GET, BlobStoreTaskPriority.NORMAL, b1.getId()).isEmpty(), "no applicable rules");

        final CacheThrottleRule getRule = BeanFactory.newBean(CacheThrottleRule.class).setRequestType(JobRequestType.GET).setMaxCachePercent(.8);
        mockDaoDriver.create(getRule);
        final CacheThrottleRule putRule = BeanFactory.newBean(CacheThrottleRule.class).setRequestType(JobRequestType.PUT).setMaxCachePercent(.8);
        mockDaoDriver.create(putRule);
        final CacheThrottleRule b1Rule = BeanFactory.newBean(CacheThrottleRule.class).setRequestType(JobRequestType.GET).setBucketId(b1.getId()).setMaxCachePercent(.1);
        mockDaoDriver.create(b1Rule);
        final CacheThrottleRule b2Rule = BeanFactory.newBean(CacheThrottleRule.class).setRequestType(JobRequestType.GET).setBucketId(b2.getId()).setMaxCachePercent(.1);
        mockDaoDriver.create(b2Rule);
        final CacheThrottleRule priorityRule = BeanFactory.newBean(CacheThrottleRule.class).setPriority(BlobStoreTaskPriority.NORMAL).setMaxCachePercent(.85);
        mockDaoDriver.create(priorityRule);

        // Gets from bucket b1
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.GET, BlobStoreTaskPriority.LOW, b1.getId(), getRule, b1Rule, priorityRule);
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.GET, BlobStoreTaskPriority.NORMAL, b1.getId(), getRule, b1Rule, priorityRule);
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.GET, BlobStoreTaskPriority.HIGH, b1.getId(), getRule, b1Rule);

        // Puts from bucket b1
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.PUT, BlobStoreTaskPriority.LOW, b1.getId(), putRule, priorityRule);
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.PUT, BlobStoreTaskPriority.NORMAL, b1.getId(), putRule, priorityRule);
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.PUT, BlobStoreTaskPriority.HIGH, b1.getId(), putRule);

        // Gets from bucket b2
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.GET, BlobStoreTaskPriority.LOW, b2.getId(), getRule, b2Rule, priorityRule);
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.GET, BlobStoreTaskPriority.NORMAL, b2.getId(), getRule, b2Rule, priorityRule);
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.GET, BlobStoreTaskPriority.HIGH, b2.getId(), getRule, b2Rule);

        // Puts from bucket b2
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.PUT, BlobStoreTaskPriority.LOW, b2.getId(), putRule, priorityRule);
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.PUT, BlobStoreTaskPriority.NORMAL, b2.getId(), putRule, priorityRule);
        verifyGetApplicableCacheThrottleRules(throttleManager, JobRequestType.PUT, BlobStoreTaskPriority.HIGH, b2.getId(), putRule);
    }

    private void verifyGetApplicableCacheThrottleRules(final CacheThrottleManager throttleManager, final JobRequestType requestType, final BlobStoreTaskPriority priority, final UUID bucketId, final CacheThrottleRule... expectedRules) {
        final Set<CacheThrottleRule> actualRules = throttleManager.getApplicableCacheThrottleRules(requestType, priority, bucketId);
        assertEquals(Arrays.stream(expectedRules).toList().size(), actualRules.size(), "number of applicable rules for: requestType=" + requestType + ", priority=" + priority + ", bucket=" + bucketId);

        for (final CacheThrottleRule expectedRule : expectedRules) {
            boolean found = false;
            for (final CacheThrottleRule actualRule : actualRules) {
                if (actualRule.toString().equals(expectedRule.toString())) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Applicable rules did not contain the expected rule: " + expectedRule.toString());
        }
    }

    @Test
    public void throttleCacheAllocation_Test() {
        final Bucket bucket = mockDaoDriver.createBucket(null, "a");
        final long totalCapacityInBytes = 100;

        final CacheThrottleRule rule = BeanFactory.newBean(CacheThrottleRule.class).setRequestType(JobRequestType.PUT).setMaxCachePercent(.85);
        mockDaoDriver.create(rule);

        final CacheThrottleManager throttleManager = spy(new CacheThrottleManager(dbSupport.getServiceManager()));
        doReturn((long)0).when(throttleManager).getNumJobEntryRowDeletes();

        // Allowed to allocate below the throttle
        final Job job1 = mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.PUT);
        final S3Object obj1 = mockDaoDriver.createObject(bucket.getId(), "o1", 80);
        final Blob blob1 = mockDaoDriver.getBlobFor(obj1.getId());
        mockDaoDriver.createJobEntry(job1.getId(), blob1);

        throttleManager.throttleCacheAllocation(job1.getRequestType(), job1.getPriority(), job1.getBucketId(), blob1.getLength(), 0, totalCapacityInBytes);
        mockDaoDriver.markBlobInCache(blob1.getId());
        TestUtil.sleep(100);
        // Allowed to allocate up to the throttle
        final Job job2 = mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.PUT);
        final S3Object obj2 = mockDaoDriver.createObject(bucket.getId(), "o2", 5);
        final Blob blob2 = mockDaoDriver.getBlobFor(obj2.getId());
        mockDaoDriver.createJobEntry(job2.getId(), blob2);

        throttleManager.throttleCacheAllocation(job2.getRequestType(), job2.getPriority(), job2.getBucketId(), blob2.getLength(), 80, totalCapacityInBytes);
        mockDaoDriver.markBlobInCache(blob2.getId());
        TestUtil.sleep(100);
        // Not allowed to allocate beyond the throttle
        final Job job3 = mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.PUT);
        final S3Object obj3 = mockDaoDriver.createObject(bucket.getId(), "o3", 5);
        final Blob blob3 = mockDaoDriver.getBlobFor(obj3.getId());
        mockDaoDriver.createJobEntry(job3.getId(), blob3);

        TestUtil.assertThrows(
                "Should have throttled",
                DataPlannerException.class,
                new TestUtil.BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        throttleManager.throttleCacheAllocation(job3.getRequestType(), job3.getPriority(), job3.getBucketId(), blob3.getLength(), 85, totalCapacityInBytes);
                    }
                } );

        // Delete job1 and verify job3 can now be allocated
        mockDaoDriver.delete(Job.class, job1);
        doReturn((long)1).when(throttleManager).getNumJobEntryRowDeletes();
        TestUtil.sleep(100);
        throttleManager.throttleCacheAllocation(job3.getRequestType(), job3.getPriority(), job3.getBucketId(), blob3.getLength(), 5, totalCapacityInBytes);

    }

    private final static Logger LOG = Logger.getLogger(CacheThrottleManager_Test.class);
    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    private MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

    @BeforeEach
    public void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }
    @AfterEach
    public void resetDB() {
        dbSupport.reset();
        mockDaoDriver = new MockDaoDriver(dbSupport);
    }

}
