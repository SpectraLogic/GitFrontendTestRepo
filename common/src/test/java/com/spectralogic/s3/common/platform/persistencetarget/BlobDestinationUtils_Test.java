/*
 *
 * Copyright C 2024, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.platform.persistencetarget;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class BlobDestinationUtils_Test {

    @Test
    public void testCreateLocalBlobDestinationsBucketIsolatedRuleSetsIsolatedBucketId() {
        final DataPersistenceRule isolatedRule = mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final S3Object obj = mockDaoDriver.createObject( bucket.getId(), "obj" );
        final Blob blob = mockDaoDriver.getBlobFor( obj.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry = mockDaoDriver.createJobEntry( job.getId(), blob );

        try (final NestableTransaction transaction = dbSupport.getServiceManager().startNestableTransaction()) {
            BlobDestinationUtils.createLocalBlobDestinations(
                    Collections.singletonList( entry ),
                    Collections.singletonList( isolatedRule ),
                    bucket.getId(),
                    transaction );
            transaction.commitTransaction();
        }

        final Set<LocalBlobDestination> destinations = dbSupport.getServiceManager()
                .getRetriever(LocalBlobDestination.class)
                .retrieveAll( Require.beanPropertyEquals(LocalBlobDestination.ENTRY_ID, entry.getId()) )
                .toSet();

        assertEquals(1, destinations.size(), "Should have created one LocalBlobDestination");
        final LocalBlobDestination dest = destinations.iterator().next();
        assertEquals(bucket.getId(), dest.getIsolatedBucketId(),
                "BUCKET_ISOLATED rule should set isolatedBucketId to the bucket's ID");
    }

    @Test
    public void testCreateLocalBlobDestinationsStandardRuleLeavesIsolatedBucketIdNull() {
        final DataPersistenceRule standardRule = mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD, dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final S3Object obj = mockDaoDriver.createObject( bucket.getId(), "obj" );
        final Blob blob = mockDaoDriver.getBlobFor( obj.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry = mockDaoDriver.createJobEntry( job.getId(), blob );

        try (final NestableTransaction transaction = dbSupport.getServiceManager().startNestableTransaction()) {
            BlobDestinationUtils.createLocalBlobDestinations(
                    Collections.singletonList( entry ),
                    Collections.singletonList( standardRule ),
                    bucket.getId(),
                    transaction );
            transaction.commitTransaction();
        }

        final Set<LocalBlobDestination> destinations = dbSupport.getServiceManager()
                .getRetriever(LocalBlobDestination.class)
                .retrieveAll( Require.beanPropertyEquals(LocalBlobDestination.ENTRY_ID, entry.getId()) )
                .toSet();

        assertEquals(1, destinations.size(), "Should have created one LocalBlobDestination");
        final LocalBlobDestination dest = destinations.iterator().next();
        assertNull(dest.getIsolatedBucketId(),
                "STANDARD isolation rule should leave isolatedBucketId null");
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    private final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
    private Bucket bucket;
    private StorageDomain sd;
    private DataPolicy dp;

    @BeforeEach
    public void setUp() {
        dbSupport.reset();
        dp = mockDaoDriver.createDataPolicy( "BlobDestinationUtils Policy" );
        bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket1" );
        sd = mockDaoDriver.createStorageDomain( "sd1" );
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
