/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.target.DataOfflineablePublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class OfflineDataStagingWindowManagerImpl_Test
{
    @Test
    public void testHappyConstruction()
    {
        new OfflineDataStagingWindowManagerImpl( new MockBeansServiceManager(), 1 );
    }
    

    @Test
    public void testTryLockSucceedsSoLongAsCapacityIsAvailable()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final OfflineDataStagingWindowManagerImpl manager =
                new OfflineDataStagingWindowManagerImpl( dbSupport.getServiceManager(), 5 );
        
        final long gb = 1024L * 1024 * 1024;
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 300 * gb );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 400 * gb );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", 800 * gb );
        final Blob blob4 = mockDaoDriver.getBlobFor( o4.getId() );
        final S3Object o5 = mockDaoDriver.createObject( null, "o5", 1600 * gb );
        final Blob blob5 = mockDaoDriver.getBlobFor( o5.getId() );
        
        final S3Target target1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        
        final JobEntry chunk1 = mockDaoDriver.createJobWithEntry( blob1);
        final JobEntry chunk2 = mockDaoDriver.createJobWithEntry( blob3 );
        final JobEntry chunk3 = mockDaoDriver.createJobWithEntry( blob4 );
        final JobEntry chunk4 = mockDaoDriver.createJobWithEntry( blob5 );

        assertTrue(manager.tryLock( S3Target.class, target1.getId(), blob1.getId() ), "Shoulda allocated 300GB of 1024GB.");
        assertEquals( false, manager.tryLock( S3Target.class, target1.getId(), blob4.getId() ), "Shoulda failed to allocate an additional 800GB." );

        assertTrue(manager.tryLock( S3Target.class, target2.getId(), blob1.getId() ), "Shoulda allocated 300GB of 1024GB.");
        assertTrue(manager.tryLock( S3Target.class, target2.getId(), blob3.getId() ), "Shoulda allocated 700GB of 1024GB.");
        assertTrue(manager.tryLock( S3Target.class, target2.getId(), blob3.getId() ), "Shoulda allocated 700GB of 1024GB.");
        assertTrue(manager.tryLock( S3Target.class, target2.getId(), blob3.getId() ), "Shoulda allocated 700GB of 1024GB.");

        manager.releaseLock( chunk1.getId() );
        assertTrue(manager.tryLock( S3Target.class, target1.getId(), chunk3.getId() ), "Shoulda allocated 800GB of 1024GB.");
        assertTrue(manager.tryLock( S3Target.class, target1.getId(), chunk3.getId() ), "Shoulda allocated 800GB of 1024GB.");

        TestUtil.sleep( 10 );
        assertEquals(false, manager.tryLock( S3Target.class, target2.getId(), blob4.getId() ), "Shoulda failed to allocate an additional 800GB.");

        mockDaoDriver.delete( JobEntry.class, chunk1 );
        mockDaoDriver.delete( JobEntry.class, chunk2 );
        TestUtil.sleep( 15 );
        assertTrue(manager.tryLock( S3Target.class, target2.getId(), blob4.getId() ), "Shoulda allocated 800GB of 1024GB.");

        mockDaoDriver.updateBean(
                target2.setOfflineDataStagingWindowInTb( 3 ), 
                DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB );
        assertTrue(manager.tryLock( S3Target.class, target2.getId(), blob5.getId() ), "Shoulda allocated 2400GB of 3072GB.");
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
