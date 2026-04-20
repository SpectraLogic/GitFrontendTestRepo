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
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class TapeFailureServiceImpl_Test 
{
    @Test
    public void testDeleteFailuresForTapeDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "a", tape1.getId() );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "b", tape2.getId() );
        
        final TapeFailureService service =
                dbSupport.getServiceManager().getService( TapeFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( TapeFailure.class )
                .setTapeId( tape1.getId() ).setType( TapeFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" )
                .setTapeDriveId( drive1.getId() ),
                null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 60 );

        final TapeFailure failure1 = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("Jason")
                .setTapeId(tape1.getId())
                .setTapeDriveId(drive1.getId())
                .setType(TapeFailureType.values()[ 1 ]);
        final TapeFailure failure2 = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("Jason")
                .setTapeId(tape2.getId())
                .setTapeDriveId(drive2.getId())
                .setType(TapeFailureType.values()[ 0 ]);
        service.create( failure1 );
        service.create( failure2 );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final TapeFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteAll( tape2.getId() );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after tape2 errors deleted.");

        service.deleteAll( tape1.getId(), TapeFailureType.values()[ 1 ] );
        assertEquals(1,  service.getCount(), "Shoulda been 1 error after tape1 error of type deleted.");
    }
    
    
    @Test
    public void testDeleteOldFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final TapeFailureService service =
                dbSupport.getServiceManager().getService( TapeFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( TapeFailure.class )
                .setTapeId( tape.getId() ).setType( TapeFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" )
                .setTapeDriveId( drive.getId() ),
                null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 600 );

        final TapeFailure failure1 = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("Jason")
                .setTapeId(tape.getId())
                .setTapeDriveId(drive.getId())
                .setType(TapeFailureType.values()[ 0 ]);
        final TapeFailure failure2 = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("Jason")
                .setTapeId(tape.getId())
                .setTapeDriveId(drive.getId())
                .setType(TapeFailureType.values()[ 0 ]);
        service.create( failure1 );
        service.create( failure2 );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final TapeFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteOldFailures( 400 );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after old errors deleted.");
    }
}
