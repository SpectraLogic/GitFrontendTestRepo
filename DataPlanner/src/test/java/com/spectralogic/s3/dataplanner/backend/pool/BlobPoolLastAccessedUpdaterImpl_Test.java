/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;



import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class BlobPoolLastAccessedUpdaterImpl_Test 
{

    @Test
    public void testConstructorNullServiceNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

             public void test()
            {
                new BlobPoolLastAccessedUpdaterImpl( null, 0 );
            }
        } );
    }
    
    
     @Test
    public void testAccessedNullBlobIdNotAllowed()
    {
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );

        final BlobPoolLastAccessedUpdater updater = new BlobPoolLastAccessedUpdaterImpl( service, 0 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    updater.accessed( null );
                }
            } );
    }
    
    
     @Test
    public void testSynchronousUpdatesWork()
    {
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o0 = mockDaoDriver.createObject( null, "o0" );
        final Blob b0 = mockDaoDriver.getBlobFor( o0.getId() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final BlobPool bp0 = mockDaoDriver.putBlobOnPool( pool1.getId(), b0.getId() );
        final BlobPool bp1 = mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        final BlobPool bp2 = mockDaoDriver.putBlobOnPool( pool1.getId(), b2.getId() );
        final BlobPool bp3 = mockDaoDriver.putBlobOnPool( pool2.getId(), b2.getId() );

        TestUtil.sleep( 10 );
        
        final BlobPoolLastAccessedUpdater updater = new BlobPoolLastAccessedUpdaterImpl( service, 0 );
        updater.accessed( b2.getId() );
        final Object expected1 = bp0.getLastAccessed();
        assertEquals(expected1, mockDaoDriver.attain( bp0 ).getLastAccessed(), "Shoulda changed b2 entries only.");
        final Object expected = bp1.getLastAccessed();
        assertEquals(expected, mockDaoDriver.attain( bp1 ).getLastAccessed(), "Shoulda changed b2 entries only.");
        assertTrue(bp2.getLastAccessed().getTime() <
                mockDaoDriver.attain( bp2 ).getLastAccessed().getTime(), "Shoulda changed b2 entries only.");
        assertTrue(bp3.getLastAccessed().getTime() <
                mockDaoDriver.attain( bp3 ).getLastAccessed().getTime(), "Shoulda changed b2 entries only.");
    }
    
    
     @Test
    public void testAsynchronousUpdatesWork()
    {
        
        final BlobPoolService service = dbSupport.getServiceManager().getService( BlobPoolService.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o0 = mockDaoDriver.createObject( null, "o0" );
        final Blob b0 = mockDaoDriver.getBlobFor( o0.getId() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final BlobPool bp0 = mockDaoDriver.putBlobOnPool( pool1.getId(), b0.getId() );
        final BlobPool bp1 = mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        final BlobPool bp2 = mockDaoDriver.putBlobOnPool( pool1.getId(), b2.getId() );
        final BlobPool bp3 = mockDaoDriver.putBlobOnPool( pool2.getId(), b2.getId() );

        TestUtil.sleep( 10 );
        
        final BlobPoolLastAccessedUpdater updater = new BlobPoolLastAccessedUpdaterImpl( service, 100 );
        updater.accessed( b2.getId() );
        final Object expected12 = bp0.getLastAccessed();
        assertEquals(expected12, mockDaoDriver.attain( bp0 ).getLastAccessed(), "Should notta updated synchronously.");
        final Object expected11 = bp1.getLastAccessed();
        assertEquals(expected11, mockDaoDriver.attain( bp1 ).getLastAccessed(), "Should notta updated synchronously.");
        final Object expected10 = bp2.getLastAccessed();
        assertEquals(expected10, mockDaoDriver.attain( bp2 ).getLastAccessed(), "Should notta updated synchronously.");
        final Object expected9 = bp3.getLastAccessed();
        assertEquals(expected9, mockDaoDriver.attain( bp3 ).getLastAccessed(), "Should notta updated synchronously.");

        int i = 100;
        while ( --i > 0 && bp3.getLastAccessed().getTime() 
                == mockDaoDriver.attain( bp3 ).getLastAccessed().getTime() )
        {
            TestUtil.sleep( 10 );
        }

        final Object expected8 = bp0.getLastAccessed();
        assertEquals(expected8, mockDaoDriver.attain( bp0 ).getLastAccessed(), "Shoulda changed b2 entries only.");
        final Object expected7 = bp1.getLastAccessed();
        assertEquals(expected7, mockDaoDriver.attain( bp1 ).getLastAccessed(), "Shoulda changed b2 entries only.");
        assertTrue(bp2.getLastAccessed().getTime() <
                mockDaoDriver.attain( bp2 ).getLastAccessed().getTime(), "Shoulda changed b2 entries only.");
        assertTrue(bp3.getLastAccessed().getTime() <
                mockDaoDriver.attain( bp3 ).getLastAccessed().getTime(), "Shoulda changed b2 entries only.");

        mockDaoDriver.attainAndUpdate( bp2 );
        mockDaoDriver.attainAndUpdate( bp3 );
        updater.accessed( b1.getId() );
        final Object expected6 = bp0.getLastAccessed();
        assertEquals(expected6, mockDaoDriver.attain( bp0 ).getLastAccessed(), "Should notta updated synchronously.");
        final Object expected5 = bp1.getLastAccessed();
        assertEquals(expected5, mockDaoDriver.attain( bp1 ).getLastAccessed(), "Should notta updated synchronously.");
        final Object expected4 = bp2.getLastAccessed();
        assertEquals(expected4, mockDaoDriver.attain( bp2 ).getLastAccessed(), "Should notta updated synchronously.");
        final Object expected3 = bp3.getLastAccessed();
        assertEquals(expected3, mockDaoDriver.attain( bp3 ).getLastAccessed(), "Should notta updated synchronously.");

        i = 100;
        while ( --i > 0 && bp1.getLastAccessed().getTime() 
                == mockDaoDriver.attain( bp1 ).getLastAccessed().getTime() )
        {
            TestUtil.sleep( 10 );
        }

        final Object expected2 = bp0.getLastAccessed();
        assertEquals(expected2, mockDaoDriver.attain( bp0 ).getLastAccessed(), "Shoulda changed b1 entry only.");
        assertTrue(bp2.getLastAccessed().getTime() <
                mockDaoDriver.attain( bp1 ).getLastAccessed().getTime(), "Shoulda changed b1 entry only.");
        final Object expected1 = bp2.getLastAccessed();
        assertEquals(expected1, mockDaoDriver.attain( bp2 ).getLastAccessed(), "Shoulda changed b1 entry only.");
        final Object expected = bp3.getLastAccessed();
        assertEquals(expected, mockDaoDriver.attain( bp3 ).getLastAccessed(), "Shoulda changed b1 entry only.");
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public  void resetDB() {
        dbSupport.reset();
    }
}
