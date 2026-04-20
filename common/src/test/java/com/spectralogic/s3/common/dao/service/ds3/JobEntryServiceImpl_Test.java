/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.ByteRangesImpl;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public class JobEntryServiceImpl_Test 
{
    @Test
    public void testGetNextChunkNumberReturnsExpectedValueWhenNoChunksExist()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final UUID jobId = new MockDaoDriver( dbSupport )
                .createJob( null, null, JobRequestType.GET ).getId();

        assertEquals(1,  dbSupport.getServiceManager().getService(JobEntryService.class)
                .getNextChunkNumber(jobId), "Shoulda returned the first chunk number in the sequence.");
    }
    
    
    @Test
    public void testGetNextChunkNumberReturnsExpectedValueWhenChunksAlreadyExist()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final JobEntryService jobEntryService = dbSupport.getServiceManager()
                .getService( JobEntryService.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID jobId = mockDaoDriver.createJob( null, null, JobRequestType.GET ).getId();
        final Bucket bucket = mockDaoDriver.createBucket(null, "test_bucket");
        final UUID objectId1 = mockDaoDriver.createObject( bucket.getId(), "test_object1", -1 ).getId();
        final UUID objectId2 = mockDaoDriver.createObject( bucket.getId(), "test_object2", -1 ).getId();
        final Blob blob1 = mockDaoDriver.createBlobs( objectId1, 1, 0L, 1234L ).iterator().next();
        final Blob blob2 = mockDaoDriver.createBlobs( objectId2, 1, 1234L, 4321L ).iterator().next();

        final JobEntry jobEntry1 = mockDaoDriver.createJobEntry( jobId, blob1 );
        final JobEntry jobEntry2 = mockDaoDriver.createJobEntry( jobId, blob2 );
        jobEntry1.setChunkNumber( 1234 );
        jobEntry2.setChunkNumber( 12 );
        jobEntryService.update(jobEntry1, JobEntry.CHUNK_NUMBER );
        jobEntryService.update(jobEntry2, JobEntry.CHUNK_NUMBER );

        assertEquals(1235,  jobEntryService.getNextChunkNumber(jobId), "Shoulda returned the next chunk number in the sequence.");
    }
    
    
    @Test
    public void testGetSizeInBytesReturnsBlobSize()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID userId = mockDaoDriver.createUser( "test_user" ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, "test_bucket" ).getId();
        final UUID jobId = mockDaoDriver.createJob( bucketId, userId, JobRequestType.GET ).getId();
        final UUID objectId = mockDaoDriver.createObject( bucketId, "test_object", -1 ).getId();
        final Blob blob1 = mockDaoDriver.createBlobs( objectId, 1, 0L, 1234L ).iterator().next();
        final Blob blob2 = mockDaoDriver.createBlobs( objectId, 1, 1234L, 4321L ).iterator().next();
        final JobEntry entry1 = mockDaoDriver.createJobEntry(jobId, blob1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry(jobId, blob2 );

        assertEquals(1234L,  dbSupport.getServiceManager().getService(JobEntryService.class)
                .getSizeInBytes(entry1.getId()), "Shoulda returned total bytes because the chunk had two blobs.");
        assertEquals(4321L,  dbSupport.getServiceManager().getService(JobEntryService.class)
                .getSizeInBytes(entry2.getId()), "Shoulda returned total bytes because the chunk had two blobs.");
    }


    @Test
    public void testDeleteRemovesJobEntriesById()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID objectId =
                mockDaoDriver.createObject( null, MockDaoDriver.DEFAULT_OBJECT_NAME, -1 ).getId();
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket");
        final Job job = mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.GET );
        final List< UUID > JobChunkIdList = new ArrayList<>(
                BeanUtils.toMap( mockDaoDriver.createJobEntries(
                        mockDaoDriver.createBlobs( objectId, 10, 12L ) ) ).keySet() );

        final JobEntryService JobEntryService =
                dbSupport.getServiceManager().getService( JobEntryService.class );
        assertEquals(10, JobEntryService.getCount(), "Shoulda had ten job entries to start with.");
        JobEntryService.delete( new HashSet<>( JobChunkIdList.subList( 0, 5 ) ) );
        assertEquals(
                new HashSet<>( JobChunkIdList.subList( 5, 10 ) ),
                BeanUtils.toMap( JobEntryService.retrieveAll().toSet() ).keySet(),
                "Shoulda had the expected job entries left.");
    }


    @Test
    public void testGetEntryForS3RequestWithSingleObjectPutJob()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket");
        final UUID objectId =
                mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 ).getId();
        final Job job = mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.PUT );
        final List< UUID > JobChunkIdList = new ArrayList<>(
                BeanUtils.toMap( mockDaoDriver.createJobEntries(
                        mockDaoDriver.createBlobs( objectId, 1, 12L ) ) ).keySet() );

        final JobEntryService JobEntryService =
                dbSupport.getServiceManager().getService( JobEntryService.class );
        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();

        assertNull(JobEntryService.getEntryForS3Request( JobRequestType.PUT, objectId,  null, false, id1 ));


        JobEntry entry = JobEntryService.getEntryForS3Request(
                JobRequestType.PUT,
                objectId,

                job.getId(),
                false,
                id1 );
        final Object actual2 = entry.getId();
        assertEquals(JobChunkIdList.get( 0 ), actual2, "Shoulda found correct entry");

        TestUtil.assertThrows( "Should not match for wrong job type", GenericFailure.BAD_REQUEST, new TestUtil.BlastContainer()
        {
            public void test()
            {
                JobEntryService.getEntryForS3Request( JobRequestType.GET, objectId,  job.getId(), false, id1 );
            }
        } );

        TestUtil.assertThrows( "Should return 404 for bogus job ID", GenericFailure.NOT_FOUND, new TestUtil.BlastContainer()
        {
            public void test()
            {
                JobEntryService.getEntryForS3Request( JobRequestType.PUT, objectId,  UUID.randomUUID(), false, id1 );
            }
        } );



        mockDaoDriver.updateBean( job.setImplicitJobIdResolution( true ), Job.IMPLICIT_JOB_ID_RESOLUTION );

        entry = JobEntryService.getEntryForS3Request(
                JobRequestType.PUT,
                objectId,

                null,
                false,
                id1 );
        final Object actual1 = entry.getId();
        assertEquals(JobChunkIdList.get( 0 ), actual1, "Shoulda found correct entry with implicit job id resolution");



    }

}
