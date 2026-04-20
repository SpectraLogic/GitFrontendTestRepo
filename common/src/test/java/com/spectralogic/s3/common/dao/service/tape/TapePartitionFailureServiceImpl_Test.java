/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class TapePartitionFailureServiceImpl_Test 
{
    @Test
    public void testDeleteFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, "tp2" );
        
        final TapePartitionFailureService service =
                dbSupport.getServiceManager().getService( TapePartitionFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( tp1.getId() ).setType( TapePartitionFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ),
                null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 60 );
        
        service.create( 
                tp1.getId(), TapePartitionFailureType.values()[ 0 ], new RuntimeException( "Jason" ), null );
        service.create( 
                tp2.getId(), TapePartitionFailureType.values()[ 1 ], "Jason", null );
        service.create( // ignored
                tp2.getId(), TapePartitionFailureType.values()[ 1 ], "Jason", Integer.valueOf( 1 ) );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final TapePartitionFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteAll( tp2.getId(), TapePartitionFailureType.values()[ 0 ] );
        assertEquals(3,  service.getCount(), "Shoulda been 3 errors after tp2 errors of type deleted.");

        service.deleteAll( tp2.getId(), TapePartitionFailureType.values()[ 1 ] );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after tp2 errors of type deleted.");
    }
    
    
    @Test
    public void testDeleteAllFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, "tp2" );
        
        final TapePartitionFailureService service =
                dbSupport.getServiceManager().getService( TapePartitionFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( tp1.getId() ).setType( TapePartitionFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ),
                null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 60 );
        
        service.create( 
                tp1.getId(), TapePartitionFailureType.values()[ 0 ], new RuntimeException( "Jason" ), null );
        service.create( 
                tp2.getId(), TapePartitionFailureType.values()[ 1 ], "Jason", null );
        service.create( // ignored
                tp2.getId(), TapePartitionFailureType.values()[ 1 ], "Jason", Integer.valueOf( 1 ) );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final TapePartitionFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteAll( tp2.getId() );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after tp2 errors deleted.");
    }
    
    
    @Test
    public void testDeleteOldFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapePartition tp = mockDaoDriver.createTapePartition( null, "tp1" );
        
        final TapePartitionFailureService service =
                dbSupport.getServiceManager().getService( TapePartitionFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( TapePartitionFailure.class )
                .setPartitionId( tp.getId() ).setType( TapePartitionFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ),
                null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 200 );
        
        service.create( 
                tp.getId(), 
                TapePartitionFailureType.values()[ 0 ],
                new RuntimeException( "Jason" ), 
                null );
        service.create(
                tp.getId(), 
                TapePartitionFailureType.values()[ 0 ],
                "Jason", 
                null );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final TapePartitionFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteOldFailures( 200 );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after old errors deleted.");
    }
}
