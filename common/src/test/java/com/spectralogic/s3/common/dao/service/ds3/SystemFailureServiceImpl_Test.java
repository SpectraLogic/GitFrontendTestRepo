/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class SystemFailureServiceImpl_Test 
{


    @Test
    public void testDeleteFailuresDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final SystemFailureService service =
                dbSupport.getServiceManager().getService( SystemFailureService.class );
        assertEquals(0,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Should notta dispatched notifications yet.");
        service.create( BeanFactory.newBean( SystemFailure.class )
                .setType( SystemFailureType.values()[ 0 ] )
                .setErrorMessage( "Jason" ),
                null );
        assertEquals(1,  dbSupport.getNotificationEventDispatcherBtih().getTotalCallCount(), "Shoulda dispatched notifications for create.");

        TestUtil.sleep( 60 );
        
        service.create( SystemFailureType.values()[ 0 ], new RuntimeException( "Jason" ), null );
        service.create( SystemFailureType.values()[ 1 ], "Jason", null );
        service.create( // ignored
                SystemFailureType.values()[ 1 ], "Jason", Integer.valueOf( 1 ) );

        assertEquals(3,  service.getCount(), "Shoulda been 3 errors initially.");
        for ( final SystemFailure error : service.retrieveAll().toSet() )
        {
            assertTrue(error.getErrorMessage().contains( "Jason" ), "Error message shoulda been correct.");
        }
        
        service.deleteAll( SystemFailureType.values()[ 0 ] );
        assertEquals(1,  service.getCount(), "Shoulda been 1 errors after errors of type deleted.");

        service.deleteAll( SystemFailureType.values()[ 1 ] );
        assertEquals(0,  service.getCount(), "Shoulda been 0 errors after errors of type deleted.");
    }
}
