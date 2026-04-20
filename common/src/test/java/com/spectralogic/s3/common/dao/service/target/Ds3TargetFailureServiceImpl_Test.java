/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class Ds3TargetFailureServiceImpl_Test 
{
    @Test
    public void testDeleteFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target sd1 = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        
        final Ds3TargetFailureService service =
                dbSupport.getServiceManager().getService( Ds3TargetFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( Ds3TargetFailure.class )
                .setTargetId( sd1.getId() ).setType( TargetFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ), null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 60 );
        
        service.create( 
                sd1.getId(), TargetFailureType.values()[ 0 ], new RuntimeException( "Jason" ), null );
        service.create( 
                sd2.getId(), TargetFailureType.values()[ 1 ], "Jason", null );
        service.create( // ignored
                sd2.getId(), TargetFailureType.values()[ 1 ], "Jason", Integer.valueOf( 1 ) );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final Ds3TargetFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteAll( sd2.getId(), TargetFailureType.values()[ 0 ] );
        assertEquals(3,  service.getCount(), "Shoulda been 3 errors after sd2 errors of type deleted.");

        service.deleteAll( sd2.getId(), TargetFailureType.values()[ 1 ] );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after sd2 errors of type deleted.");
    }
    
    
    @Test
    public void testDeleteAllFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target sd1 = mockDaoDriver.createDs3Target( "sd1" );
        final Ds3Target sd2 = mockDaoDriver.createDs3Target( "sd2" );
        
        final Ds3TargetFailureService service =
                dbSupport.getServiceManager().getService( Ds3TargetFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( Ds3TargetFailure.class )
                .setTargetId( sd1.getId() ).setType( TargetFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ), null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 60 );
        
        service.create( 
                sd1.getId(), TargetFailureType.values()[ 0 ], new RuntimeException( "Jason" ), null );
        service.create( 
                sd2.getId(), TargetFailureType.values()[ 1 ], "Jason", null );
        service.create( // ignored
                sd2.getId(), TargetFailureType.values()[ 1 ], "Jason", Integer.valueOf( 1 ) );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final Ds3TargetFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteAll( sd2.getId() );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after sd2 errors deleted.");
    }
    
    
    @Test
    public void testDeleteOldFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target sd = mockDaoDriver.createDs3Target( "sd1" );
        
        final Ds3TargetFailureService service =
                dbSupport.getServiceManager().getService( Ds3TargetFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( Ds3TargetFailure.class )
                .setTargetId( sd.getId() ).setType( TargetFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ), null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 400 );
        
        service.create( 
                sd.getId(), 
                TargetFailureType.values()[ 0 ],
                new RuntimeException( "Jason" ), 
                null );
        service.create(
                sd.getId(), 
                TargetFailureType.values()[ 0 ],
                "Jason", 
                null );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final Ds3TargetFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteOldFailures( 400 );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after old errors deleted.");
    }
}
