/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class PoolFailureServiceImpl_Test 
{
    @Test
    public void testDeleteFailuresForPoolDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        
        final PoolFailureService service =
                dbSupport.getServiceManager().getService( PoolFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( PoolFailure.class )
                .setPoolId( pool1.getId() ).setType( PoolFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ),
                null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 60 );
        
        service.create( pool1.getId(), PoolFailureType.values()[ 1 ], new RuntimeException( "Jason" ) );
        service.create( pool2.getId(), PoolFailureType.values()[ 0 ], "Jason" );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final PoolFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteAll( pool2.getId() );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after pool2 errors deleted.");

        service.deleteAll( pool1.getId(), PoolFailureType.values()[ 0 ] );
        assertEquals(1,  service.getCount(), "Shoulda been 1 error after pool1 error deleted.");

        service.deleteAll( pool1.getId(), PoolFailureType.values()[ 0 ] );
        assertEquals(1,  service.getCount(), "Shoulda been 1 error after pool1 error deleted.");

        service.deleteAll( pool1.getId(), PoolFailureType.values()[ 1 ] );
        assertEquals(0,  service.getCount(), "Shoulda been no error after pool1 error deleted.");
    }
    
    
    @Test
    public void testDeleteOldFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        final PoolFailureService service =
                dbSupport.getServiceManager().getService( PoolFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( PoolFailure.class )
                .setPoolId( pool.getId() ).setType( PoolFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ),
                null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 600 );
        
        service.create( pool.getId(), PoolFailureType.values()[ 0 ], new RuntimeException( "Jason" ) );
        service.create( pool.getId(), PoolFailureType.values()[ 0 ], "Jason" );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final PoolFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteOldFailures( 400 );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after old errors deleted.");
    }
}
