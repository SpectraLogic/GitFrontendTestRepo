/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class BlobPoolReadLockReleaser_Test
{
    @Test
    public void testConstructorNullLockSupportNotAllowed()
    {
        
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobPoolReadLockReleaser( 
                        null,
                        dbSupport.getServiceManager(),
                        50 );
            }
        } );
    }
    

    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobPoolReadLockReleaser( 
                        createLockSupport(),
                        null,
                        50 );
            }
        } );
    }
    
    
    @Test
    public void testStaleBlobReadLocksCleanedUp()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final BlobPoolReadLockReleaser releaser = new BlobPoolReadLockReleaser( 
                lockSupport,
                dbSupport.getServiceManager(),
                100 );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        
        mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        mockDaoDriver.createJobWithEntry( JobRequestType.VERIFY, b2 );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b3, b4 );
        mockDaoDriver.createJobWithEntry( JobRequestType.GET, b3 );
        
        final UUID poolId = UUID.randomUUID();
        lockSupport.acquireReadLock( poolId, b1.getId() );
        lockSupport.acquireReadLock( poolId, b2.getId() );
        lockSupport.acquireReadLock( poolId, b3.getId() );
        lockSupport.acquireReadLock( poolId, b4.getId() );
        final Object expected1 = CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.getId() );
        assertEquals(expected1, lockSupport.getBlobLockHolders(), "Should notta released stale locks yet.");

        releaser.schedule();
        int i = 20;
        while ( --i > 0 && 2 != lockSupport.getBlobLockHolders().size() )
        {
            TestUtil.sleep( 50 );
        }
        final Object expected = CollectionFactory.toSet( b3.getId(), b4.getId() );
        assertEquals(expected, lockSupport.getBlobLockHolders(), "Shoulda released stale locks.");
    }
    
    
    private PoolLockSupport< PoolTask > createLockSupport()
    {
        return new PoolLockSupportImpl<>(
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ), 
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
    }

    private static DatabaseSupport dbSupport ;
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterAll
    public static void resetDB() {
        dbSupport.reset();
    }
}
