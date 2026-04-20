/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainFailureService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class WritesStalledSupport_Test 
{
    @Test
    public void testWritesStalledNullStorageDomainIdNotAllowed()
    {
        
        final WritesStalledSupport support = WritesStalledSupport.INSTANCE;
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition pp = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "pp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), pp.getId() );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    support.stalled( null, null, dbSupport.getServiceManager() );
                }
            } );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    support.stalled( null, UUID.randomUUID(), dbSupport.getServiceManager() );
                }
            } );

        support.stalled( null, sd.getId(), dbSupport.getServiceManager() );
    }


    @Test
    public void testWritesNotStalledNullStorageDomainIdNotAllowed()
    {
        
        final WritesStalledSupport support = WritesStalledSupport.INSTANCE;
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition pp = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "pp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), pp.getId() );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    support.notStalled( null, null, dbSupport.getServiceManager() );
                }
            } );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    support.notStalled( null, UUID.randomUUID(), dbSupport.getServiceManager() );
                }
            } );

        support.notStalled( null, sd.getId(), dbSupport.getServiceManager() );
    }


    @Test
    public void testStallRecordedAndUnrecordedAsStallsOccurWhenPoolStorageOnly()
    {
        
        final WritesStalledSupport support = WritesStalledSupport.INSTANCE;
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition pp = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "pp1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), pp.getId() );

        final StorageDomainFailureService service =
                dbSupport.getServiceManager().getService( StorageDomainFailureService.class );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");
    }


    @Test
    public void testStallRecordedAndUnrecordedAsStallsOccurWhenTapeStorageOnly()
    {
        
        final WritesStalledSupport support = WritesStalledSupport.INSTANCE;
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final TapePartition tp = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), tp.getId(), TapeType.LTO6 );

        final StorageDomainFailureService service =
                dbSupport.getServiceManager().getService( StorageDomainFailureService.class );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( tp.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( tp.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");
    }


    @Test
    public void testStallRecordedAndUnrecordedAsStallsOccurWhenTapeAndPoolStorage()
    {
        
        final WritesStalledSupport support = WritesStalledSupport.INSTANCE;
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition pp = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "pp1" );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), pp.getId() );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), tp2.getId(), TapeType.LTO6 );
        
        final StorageDomainFailureService service = 
                dbSupport.getServiceManager().getService( StorageDomainFailureService.class );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp1.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp2.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( tp1.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp1.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( tp1.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( tp2.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp1.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp2.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( null, sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( tp1.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.stalled( tp1.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(1,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");

        support.notStalled( tp1.getId(), sd.getId(), dbSupport.getServiceManager() );
        assertEquals(0,  service.getCount(), "Shoulda recorded and unrecorded stall failures correctly.");
    }

    private static DatabaseSupport dbSupport;
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
