package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalJobEntryWorkServiceImpl_Test {

    @Test
    public void testRetrievalWorks() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        mockDaoDriver.createABMConfigDualCopyOnTape();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1024 * 1024 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createPersistenceTargetsForChunk(entry1.getId());
        final LocalJobEntryWorkService service = mockDaoDriver.getServiceManager().getService(LocalJobEntryWorkService.class);
        List<LocalJobEntryWork> list = service.retrieveAll().toList();
        assertEquals(2, list.size());
        assertEquals(job.getId(), list.get(0).getJobId());
        assertEquals(b1.getId(), list.get(0).getBlobId());
        assertEquals(o1.getId(), list.get(0).getObjectId());

        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 1024 * 1024 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.createPersistenceTargetsForChunk(entry2.getId());

        list = service.retrieveAll().toList();
        assertEquals(4, list.size());
        list = service.retrieveAll(Require.beanPropertyEquals(Blob.OBJECT_ID, o1.getId())).toList();
        assertEquals(2, list.size());
        assertEquals(job.getId(), list.get(0).getJobId());
        assertEquals(b1.getId(), list.get(0).getBlobId());
        assertEquals(o1.getId(), list.get(0).getObjectId());

        list = service.retrieveAll(Require.beanPropertyEquals(Blob.OBJECT_ID, UUID.randomUUID())).toList();
        assertEquals(0, list.size());

        assertThrows(RuntimeException.class, () -> {
            service.retrieveAll().toMap();
        }, "Should throw when attempting to map results that have redundant entries for the ID");

    }


    @Test
    public void testOrderByPriority() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver driver = new MockDaoDriver(dbSupport);
        driver.createABMConfigSingleCopyOnTape();

        // Create jobs with different priorities — insert in reverse order (low priority first)
        final Job lowJob = driver.createJob(null, null, JobRequestType.PUT);
        driver.updateBean(lowJob.setPriority(BlobStoreTaskPriority.LOW), Job.PRIORITY);
        final S3Object oLow = driver.createObject(null, "oLow", 100);
        driver.createJobEntry(lowJob.getId(), driver.getBlobFor(oLow.getId()));

        final Job normalJob = driver.createJob(null, null, JobRequestType.PUT);
        driver.updateBean(normalJob.setPriority(BlobStoreTaskPriority.NORMAL), Job.PRIORITY);
        final S3Object oNormal = driver.createObject(null, "oNormal", 100);
        driver.createJobEntry(normalJob.getId(), driver.getBlobFor(oNormal.getId()));

        final Job urgentJob = driver.createJob(null, null, JobRequestType.PUT);
        driver.updateBean(urgentJob.setPriority(BlobStoreTaskPriority.URGENT), Job.PRIORITY);
        final S3Object oUrgent = driver.createObject(null, "oUrgent", 100);
        driver.createJobEntry(urgentJob.getId(), driver.getBlobFor(oUrgent.getId()));

        // Create persistence targets so the view has rows
        for (final JobEntry entry : driver.retrieveAll(JobEntry.class)) {
            driver.createPersistenceTargetsForChunk(entry.getId());
        }

        final List<LocalJobEntryWork> results = dbSupport.getServiceManager()
                .getRetriever(LocalJobEntryWork.class)
                .retrieveAll(Query.where(Require.nothing()).orderBy(new BeanSQLOrdering()))
                .toList();

        assertEquals(3, results.size());
        assertEquals(BlobStoreTaskPriority.URGENT, results.get(0).getPriority(),
                "Highest priority (URGENT) should be first");
        assertEquals(BlobStoreTaskPriority.NORMAL, results.get(1).getPriority(),
                "NORMAL priority should be second");
        assertEquals(BlobStoreTaskPriority.LOW, results.get(2).getPriority(),
                "Lowest priority (LOW) should be last");
    }


    @Test
    public void testOrderByCreatedAtBreaksPriorityTie() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver driver = new MockDaoDriver(dbSupport);
        driver.createABMConfigSingleCopyOnTape();

        // Create two jobs with the same priority but different creation times
        final long now = System.currentTimeMillis();

        final Job newerJob = driver.createJob(null, null, JobRequestType.PUT);
        driver.updateBean(newerJob.setPriority(BlobStoreTaskPriority.NORMAL)
                .setCreatedAt(new Date(now)), Job.PRIORITY, Job.CREATED_AT);
        final S3Object oNewer = driver.createObject(null, "oNewer", 100);
        driver.createJobEntry(newerJob.getId(), driver.getBlobFor(oNewer.getId()));

        final Job olderJob = driver.createJob(null, null, JobRequestType.PUT);
        driver.updateBean(olderJob.setPriority(BlobStoreTaskPriority.NORMAL)
                .setCreatedAt(new Date(now - 10000)), Job.PRIORITY, Job.CREATED_AT);
        final S3Object oOlder = driver.createObject(null, "oOlder", 100);
        driver.createJobEntry(olderJob.getId(), driver.getBlobFor(oOlder.getId()));

        for (final JobEntry entry : driver.retrieveAll(JobEntry.class)) {
            driver.createPersistenceTargetsForChunk(entry.getId());
        }

        final List<LocalJobEntryWork> results = dbSupport.getServiceManager()
                .getRetriever(LocalJobEntryWork.class)
                .retrieveAll(Query.where(Require.nothing()).orderBy(new BeanSQLOrdering()))
                .toList();

        assertEquals(2, results.size());
        // createdAt sorts ascending, so older job should come first
        assertEquals(olderJob.getId(), results.get(0).getJobId(),
                "Older job should come first when priority is the same (createdAt ascending)");
        assertEquals(newerJob.getId(), results.get(1).getJobId(),
                "Newer job should come second when priority is the same (createdAt ascending)");
    }


    @Test
    public void testOrderByChunkNumberBreaksJobTie() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver driver = new MockDaoDriver(dbSupport);
        driver.createABMConfigSingleCopyOnTape();

        // Create one job with multiple blobs — entries will get sequential chunk numbers
        final Job job = driver.createJob(null, null, JobRequestType.PUT);

        final S3Object o1 = driver.createObject(null, "obj1", 100);
        final S3Object o2 = driver.createObject(null, "obj2", 200);
        final S3Object o3 = driver.createObject(null, "obj3", 300);
        final Blob b1 = driver.getBlobFor(o1.getId());
        final Blob b2 = driver.getBlobFor(o2.getId());
        final Blob b3 = driver.getBlobFor(o3.getId());

        // Create entries — chunk numbers are auto-incremented
        final JobEntry e1 = driver.createJobEntry(job.getId(), b1);
        final JobEntry e2 = driver.createJobEntry(job.getId(), b2);
        final JobEntry e3 = driver.createJobEntry(job.getId(), b3);

        for (final JobEntry entry : driver.retrieveAll(JobEntry.class)) {
            driver.createPersistenceTargetsForChunk(entry.getId());
        }

        final List<LocalJobEntryWork> results = dbSupport.getServiceManager()
                .getRetriever(LocalJobEntryWork.class)
                .retrieveAll(Query.where(Require.nothing()).orderBy(new BeanSQLOrdering()))
                .toList();

        assertEquals(3, results.size());
        assertTrue(results.get(0).getChunkNumber() < results.get(1).getChunkNumber(),
                "Chunk numbers should be in ascending order");
        assertTrue(results.get(1).getChunkNumber() < results.get(2).getChunkNumber(),
                "Chunk numbers should be in ascending order");
    }


    @Test
    public void testFullSortOrderWithAllTiebreakers() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver driver = new MockDaoDriver(dbSupport);
        driver.createABMConfigSingleCopyOnTape();

        final long now = System.currentTimeMillis();

        // Job A: NORMAL priority, older timestamp, 2 entries
        final Job jobA = driver.createJob(null, null, JobRequestType.PUT);
        driver.updateBean(jobA.setPriority(BlobStoreTaskPriority.NORMAL)
                .setCreatedAt(new Date(now - 10000)), Job.PRIORITY, Job.CREATED_AT);
        final S3Object oA1 = driver.createObject(null, "oA1", 100);
        final S3Object oA2 = driver.createObject(null, "oA2", 200);
        final JobEntry eA1 = driver.createJobEntry(jobA.getId(), driver.getBlobFor(oA1.getId()));
        final JobEntry eA2 = driver.createJobEntry(jobA.getId(), driver.getBlobFor(oA2.getId()));

        // Job B: NORMAL priority, newer timestamp, 1 entry
        final Job jobB = driver.createJob(null, null, JobRequestType.PUT);
        driver.updateBean(jobB.setPriority(BlobStoreTaskPriority.NORMAL)
                .setCreatedAt(new Date(now)), Job.PRIORITY, Job.CREATED_AT);
        final S3Object oB1 = driver.createObject(null, "oB1", 100);
        final JobEntry eB1 = driver.createJobEntry(jobB.getId(), driver.getBlobFor(oB1.getId()));

        // Job C: URGENT priority, newest timestamp, 1 entry
        final Job jobC = driver.createJob(null, null, JobRequestType.PUT);
        driver.updateBean(jobC.setPriority(BlobStoreTaskPriority.URGENT)
                .setCreatedAt(new Date(now + 10000)), Job.PRIORITY, Job.CREATED_AT);
        final S3Object oC1 = driver.createObject(null, "oC1", 100);
        final JobEntry eC1 = driver.createJobEntry(jobC.getId(), driver.getBlobFor(oC1.getId()));

        for (final JobEntry entry : driver.retrieveAll(JobEntry.class)) {
            driver.createPersistenceTargetsForChunk(entry.getId());
        }

        final List<LocalJobEntryWork> results = dbSupport.getServiceManager()
                .getRetriever(LocalJobEntryWork.class)
                .retrieveAll(Query.where(Require.nothing()).orderBy(new BeanSQLOrdering()))
                .toList();

        // Expected order: C (URGENT), A entry 1, A entry 2 (NORMAL/older), B (NORMAL/newer)
        assertEquals(4, results.size());

        // First: Job C (URGENT — highest priority regardless of timestamp)
        assertEquals(jobC.getId(), results.get(0).getJobId(),
                "URGENT priority job should come first");

        // Second & third: Job A entries (NORMAL, but older createdAt than B)
        assertEquals(jobA.getId(), results.get(1).getJobId(),
                "NORMAL priority with older timestamp should come before newer");
        assertEquals(jobA.getId(), results.get(2).getJobId(),
                "Both entries from job A should be grouped together");
        assertTrue(results.get(1).getChunkNumber() < results.get(2).getChunkNumber(),
                "Entries within same job should be ordered by chunk number ascending");

        // Fourth: Job B (NORMAL, newer createdAt)
        assertEquals(jobB.getId(), results.get(3).getJobId(),
                "NORMAL priority with newer timestamp should come last among NORMAL jobs");
    }
}