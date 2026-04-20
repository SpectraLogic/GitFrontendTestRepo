package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DetailedJobEntryServiceImpl_Test {

    @Test
    public void testRetrievalWorks() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1024 * 1024 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.createJobEntries( job.getId(),  b1 );
        mockDaoDriver.markBlobInCache(b1.getId());
        final DetailedJobEntryService service = mockDaoDriver.getServiceManager().getService(DetailedJobEntryService.class);
        List<DetailedJobEntry> list = service.retrieveAll().toList();
        assertEquals(1, list.size());
        assertEquals(job.getId(), list.get(0).getJobId());
        assertEquals(b1.getId(), list.get(0).getBlobId());
        assertEquals(o1.getId(), list.get(0).getObjectId());
        assertEquals(CacheEntryState.IN_CACHE, list.get(0).getCacheState());

        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 1024 * 1024 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobEntries( job2.getId(),  b2 );

        list = service.retrieveAll().toList();
        assertEquals(2, list.size());

        list = service.retrieveAll(Require.beanPropertyEquals(Blob.OBJECT_ID, o1.getId())).toList();
        assertEquals(1, list.size());
        assertEquals(job.getId(), list.get(0).getJobId());
        assertEquals(b1.getId(), list.get(0).getBlobId());
        assertEquals(o1.getId(), list.get(0).getObjectId());

        list = service.retrieveAll(Require.beanPropertyEquals(Blob.OBJECT_ID, UUID.randomUUID())).toList();
        assertEquals(0, list.size());
    }


}
