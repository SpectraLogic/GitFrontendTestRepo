/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.service.ds3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class StorageDomainFailureServiceImpl_Test 
{
    @Test
    public void testDeleteFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        
        final StorageDomainFailureService service =
                dbSupport.getServiceManager().getService( StorageDomainFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( StorageDomainFailure.class )
                .setStorageDomainId( sd1.getId() ).setType( StorageDomainFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ), null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        service.create(
                sd1.getId(), StorageDomainFailureType.values()[ 0 ], new RuntimeException( "Jason" ), null );
        service.create( 
                sd2.getId(), StorageDomainFailureType.values()[ 1 ], "Jason", null );
        service.create( // ignored
                sd2.getId(), StorageDomainFailureType.values()[ 1 ], "Jason", 1 );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final StorageDomainFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteAll( sd2.getId(), StorageDomainFailureType.values()[ 0 ] );
        assertEquals(3,  service.getCount(), "Shoulda been 3 errors after sd2 errors of type deleted.");

        service.deleteAll( sd2.getId(), StorageDomainFailureType.values()[ 1 ] );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after sd2 errors of type deleted.");
    }
    
    
    @Test
    public void testDeleteAllFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        
        final StorageDomainFailureService service =
                dbSupport.getServiceManager().getService( StorageDomainFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( StorageDomainFailure.class )
                .setStorageDomainId( sd1.getId() ).setType( StorageDomainFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ), null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        service.create(
                sd1.getId(), StorageDomainFailureType.values()[ 0 ], new RuntimeException( "Jason" ), null );
        service.create( 
                sd2.getId(), StorageDomainFailureType.values()[ 1 ], "Jason", null );
        service.create( // ignored
                sd2.getId(), StorageDomainFailureType.values()[ 1 ], "Jason", 1 );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final StorageDomainFailure error : service.retrieveAll().toSet() )
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
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        
        final StorageDomainFailureService service =
                dbSupport.getServiceManager().getService( StorageDomainFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( StorageDomainFailure.class )
                .setStorageDomainId( sd.getId() ).setType( StorageDomainFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ), null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 2 );
        final Duration duration = new Duration();
        
        service.create( 
                sd.getId(), 
                StorageDomainFailureType.values()[ 0 ],
                new RuntimeException( "Jason" ), 
                null );
        service.create(
                sd.getId(), 
                StorageDomainFailureType.values()[ 0 ],
                "Jason", 
                null );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final StorageDomainFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteOldFailures( duration.getElapsedMillis() + 1 );
        assertEquals(2,  service.getCount(), "Shoulda been 2 errors after old errors deleted.");
    }
}
