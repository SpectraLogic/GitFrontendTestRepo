/*
 *
 * Copyright C 2024, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PoolWorkAggregationUtils_Test {

    @Test
    public void testDiscoverPoolWorkAggregatedSingleOversizedEntryStillReturnsDirective() {
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), null );

        // A single blob larger than MAX_BYTES_PER_TASK (100 GB).
        final long oversizedLength = 100L * 1024L * 1024L * 1024L + 1L;
        final S3Object bigObj = mockDaoDriver.createObject( bucket.getId(), "bigobj", oversizedLength );
        final Blob bigBlob = mockDaoDriver.getBlobFor( bigObj.getId() );

        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.NORMAL), Job.PRIORITY);
        final JobEntry entry = mockDaoDriver.createJobEntry( job.getId(), bigBlob );

        mockDaoDriver.createLocalBlobDestinations( Arrays.asList(entry), Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache( bigBlob.getId() );
        mockDiskManager.blobLoadedToCache( bigBlob.getId() );

        final List<IODirective> result = PoolWorkAggregationUtils.discoverPoolWorkAggregated( dbSupport.getServiceManager() );

        assertNotNull(result);
        assertEquals(1, result.size(), "A single oversized entry should still produce a directive");
        assertEquals(1, result.get(0).getEntries().size(), "Directive should contain the oversized entry");
        assertEquals(entry.getId(), result.get(0).getEntries().iterator().next().getId());
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    private final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
    private static final String DATA_POLICY_NAME = "Pool Policy";
    private Bucket bucket;
    private StorageDomain sd;
    private DataPolicy dp;
    private DataPersistenceRule rule;

    @BeforeEach
    public void setUp() {
        dbSupport.reset();
        dp = mockDaoDriver.createDataPolicy( DATA_POLICY_NAME );
        bucket = mockDaoDriver.createBucket( null, dp.getId(),"bucket1" );
        sd = mockDaoDriver.createStorageDomain( "sd1" );
        rule = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
    }

    @BeforeAll
    public static void setupDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
