/*
 *
 * Copyright C 2024, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.BucketIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public final class BaseBlobTask_Test {

    @Test
    public void testConstructObjectsIoRequestDeduplicatesEntriesForSameBlobId() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        // Create a bucket and object with a single blob
        final S3Object o = mockDaoDriver.createObject(null, "testObject");
        final Blob blob = mockDaoDriver.getBlobFor(o.getId());

        // Create two separate jobs (it's not valid to have the same blob twice in one job)
        final Job job1 = mockDaoDriver.createJob(null, null, JobRequestType.PUT);
        final Job job2 = mockDaoDriver.createJob(null, null, JobRequestType.PUT);

        // Create a cache filesystem (required for constructObjectsIoRequest)
        new MockCacheFilesystemDriver(dbSupport);

        // Create two JobEntry objects from different jobs that reference the SAME blob ID
        final JobEntry entry1 = mockDaoDriver.createJobEntry(job1.getId(), blob);
        final JobEntry entry2 = mockDaoDriver.createJobEntry(job2.getId(), blob);

        // Create a set containing both entries
        final Set<JobEntry> jobEntries = new HashSet<>();
        jobEntries.add(entry1);
        jobEntries.add(entry2);

        // Create a tape (required for BaseBlobTask constructor)
        final Tape tape = mockDaoDriver.createTape(null, TapeState.NORMAL);

        // Create an anonymous subclass of BaseBlobTask to test the protected method
        final BaseBlobTask task = new BaseBlobTask(
                BlobStoreTaskPriority.HIGH,
                tape.getId(),
                new MockDiskManager(dbSupport.getServiceManager()),
                new TapeFailureManagement(dbSupport.getServiceManager()),
                dbSupport.getServiceManager()) {
            @Override
            public String getDescription() {
                return "";
            }

            @Override
            public boolean canUseTapeAlreadyInDrive(TapeAvailability tapeAvailability) {
                return false;
            }

            @Override
            public boolean canUseAvailableTape(TapeAvailability tapeAvailability) {
                return false;
            }

            @Override
            protected BlobStoreTaskState runInternal() {
                return null;
            }
            // Anonymous subclass - no additional methods needed
        };

        // Call constructObjectsIoRequest
        final S3ObjectsIoRequest request = task.constructObjectsIoRequestFromJobEntries(JobRequestType.PUT, jobEntries);

        // Verify the request structure
        assertNotNull(request, "Request should not be null");
        assertNotNull(request.getBuckets(), "Buckets should not be null");
        assertEquals(1, request.getBuckets().length, "Should have exactly one bucket");

        final BucketIoRequest bucketRequest = request.getBuckets()[0];
        assertNotNull(bucketRequest.getObjects(), "Objects should not be null");
        assertEquals(1, bucketRequest.getObjects().length, "Should have exactly one object");

        final S3ObjectIoRequest objectRequest = bucketRequest.getObjects()[0];
        assertNotNull(objectRequest.getBlobs(), "Blobs should not be null");

        // The key assertion: despite having 2 JobEntry objects with the same blob ID,
        // the resulting request should only contain the blob ONCE
        assertEquals(1, objectRequest.getBlobs().length,
                "Should have exactly one blob (deduplicated), not two");

        final BlobIoRequest blobRequest = objectRequest.getBlobs()[0];
        assertEquals(blob.getId(), blobRequest.getId(),
                "Blob ID should match the original blob");
    }

    @Test
    public void testConstructObjectsIoRequestSortsByOrderIndex() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final S3Object object1 = mockDaoDriver.createObject(null, "object1");
        final S3Object object2 = mockDaoDriver.createObject(null, "object2");
        final S3Object object3 = mockDaoDriver.createObject(null, "object3");

        final Blob blob1 = mockDaoDriver.getBlobFor(object1.getId());
        final Blob blob2a = mockDaoDriver.getBlobFor(object2.getId());
        // Object2 has two blobs with different byte offsets
        final Blob blob2b = mockDaoDriver.createBlobsAtOffset(object2.getId(), 1, 100L, 1000L).get(0);
        final Blob blob3 = mockDaoDriver.getBlobFor(object3.getId());

        final Tape tape = mockDaoDriver.createTape(null, TapeState.NORMAL);

        // Put blobs on tape: blob3, blob2b (higher offset), blob1, blob2a (lower offset)
        // Object order by first blob's order_index: object3=1, object2=2 (blob2b), object1=3
        // Within object2, blobs sorted by byte offset: blob2a (offset 0), then blob2b (offset 1000)
        mockDaoDriver.putBlobOnTape(tape.getId(), blob3.getId());
        mockDaoDriver.putBlobOnTape(tape.getId(), blob2b.getId());
        mockDaoDriver.putBlobOnTape(tape.getId(), blob1.getId());
        mockDaoDriver.putBlobOnTape(tape.getId(), blob2a.getId());

        final Job job = mockDaoDriver.createJob(null, null, JobRequestType.GET);
        final JobEntry entry1 = mockDaoDriver.createJobEntry(job.getId(), blob1);
        final JobEntry entry2a = mockDaoDriver.createJobEntry(job.getId(), blob2a);
        final JobEntry entry2b = mockDaoDriver.createJobEntry(job.getId(), blob2b);
        final JobEntry entry3 = mockDaoDriver.createJobEntry(job.getId(), blob3);

        mockDaoDriver.updateBean(entry1.setReadFromTapeId(tape.getId()), LocalJobEntryWork.READ_FROM_TAPE_ID);
        mockDaoDriver.updateBean(entry2a.setReadFromTapeId(tape.getId()), LocalJobEntryWork.READ_FROM_TAPE_ID);
        mockDaoDriver.updateBean(entry2b.setReadFromTapeId(tape.getId()), LocalJobEntryWork.READ_FROM_TAPE_ID);
        mockDaoDriver.updateBean(entry3.setReadFromTapeId(tape.getId()), LocalJobEntryWork.READ_FROM_TAPE_ID);

        new MockCacheFilesystemDriver(dbSupport);

        final Set<JobEntry> jobEntries = new HashSet<>();
        jobEntries.add(entry1);
        jobEntries.add(entry2a);
        jobEntries.add(entry2b);
        jobEntries.add(entry3);

        final BaseBlobTask task = new BaseBlobTask(
                BlobStoreTaskPriority.HIGH,
                tape.getId(),
                new MockDiskManager(dbSupport.getServiceManager()),
                new TapeFailureManagement(dbSupport.getServiceManager()),
                dbSupport.getServiceManager()) {
            @Override
            public String getDescription() {
                return "";
            }

            @Override
            public boolean canUseTapeAlreadyInDrive(TapeAvailability tapeAvailability) {
                return false;
            }

            @Override
            public boolean canUseAvailableTape(TapeAvailability tapeAvailability) {
                return false;
            }

            @Override
            protected BlobStoreTaskState runInternal() {
                return null;
            }
        };

        S3ObjectsIoRequest request = task.constructObjectsIoRequestFromJobEntries(JobRequestType.PUT, jobEntries);

        assertNotNull(request);
        assertEquals(1, request.getBuckets().length);

        BucketIoRequest bucketRequest = request.getBuckets()[0];
        assertEquals(3, bucketRequest.getObjects().length);

        S3ObjectIoRequest[] objectRequests = bucketRequest.getObjects();

        // Within object2, blobs sorted by byte offset despite blob2a having higher order_index
        BlobIoRequest[] object2Blobs = objectRequests[1].getBlobs();
        assertEquals(2, object2Blobs.length);
        assertEquals(blob2a.getId(), object2Blobs[0].getId());
        assertEquals(blob2b.getId(), object2Blobs[1].getId());


        request = task.constructObjectsIoRequestFromJobEntries(JobRequestType.GET, jobEntries);

        assertNotNull(request);
        assertEquals(1, request.getBuckets().length);

        bucketRequest = request.getBuckets()[0];
        assertEquals(3, bucketRequest.getObjects().length);

        objectRequests = bucketRequest.getObjects();

        // Objects sorted by tape order_index of their first blob on tape
        assertEquals(object3.getId(), objectRequests[0].getId());
        assertEquals(object2.getId(), objectRequests[1].getId());
        assertEquals(object1.getId(), objectRequests[2].getId());

        // Within object2, blobs are NOT sorted by byte offset because we are doing a get
        object2Blobs = objectRequests[1].getBlobs();
        assertEquals(2, object2Blobs.length);
        assertEquals(blob2b.getId(), object2Blobs[0].getId());
        assertEquals(blob2a.getId(), object2Blobs[1].getId());
    }
}