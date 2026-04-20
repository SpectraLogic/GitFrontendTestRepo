package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DetailedLocalBlobDestinationServiceImpl_Test {

    @Test
    public void testRetrievalWorks() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1024 * 1024 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry entry = mockDaoDriver.createJobEntry( job.getId(),  b1 );
        mockDaoDriver.createPersistenceTargetsForChunk( entry.getId() );
        final DetailedLocalBlobDestinationService service = mockDaoDriver.getServiceManager().getService(DetailedLocalBlobDestinationService.class);
        final List<DetailedLocalBlobDestination> list = service.retrieveAll().toList();
        assertEquals(1, list.size());
        assertEquals(job.getId(), list.get(0).getJobId());
        assertEquals(entry.getBlobStoreState(), list.get(0).getBlobStoreState());
        assertEquals(job.getPriority(), list.get(0).getPriority());
        assertEquals(job.getCreatedAt(), list.get(0).getCreatedAt());

    }


}
