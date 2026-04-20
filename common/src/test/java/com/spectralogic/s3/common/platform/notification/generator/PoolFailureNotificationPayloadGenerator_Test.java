/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.platform.notification.domain.payload.PoolFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class PoolFailureNotificationPayloadGenerator_Test 
{
    @Test
    public void testConstructorNullArgumentNotAllowed()
    {
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class,
                new BlastContainer()
                {
                    @Override

                    public void test() throws Throwable
                    {
                        new PoolFailureNotificationPayloadGenerator( null );
                    }
                } );
    }
    
    
    @Test
    public void testGenerateNotificationEventReturnsRelevantEvent()
    {
        final PoolFailure poolFailure = BeanFactory.newBean( PoolFailure.class );
        poolFailure.setDate( new Date() );
        poolFailure.setErrorMessage( "oops, there was an error" );
        poolFailure.setPoolId( UUID.randomUUID() );
        poolFailure.setType( PoolFailureType.INSPECT_FAILED );
        final NotificationPayload event = new PoolFailureNotificationPayloadGenerator( poolFailure )
                .generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertTrue(
                PoolFailureNotificationPayload.class.isAssignableFrom( event.getClass() ),
                "Shoulda returned a pool failure notification event."
                );
        final PoolFailureNotificationPayload poolFailureNotificationEvent =
                (PoolFailureNotificationPayload)event;
        final Object expected3 = poolFailure.getDate();
        assertEquals(expected3, poolFailureNotificationEvent.getDate(), "Shoulda returned the same date as provided.");
        final Object expected2 = poolFailure.getErrorMessage();
        assertEquals(expected2, poolFailureNotificationEvent.getErrorMessage(), "Shoulda returned the same error message as provided.");
        final Object expected1 = poolFailure.getPoolId();
        assertEquals(expected1, poolFailureNotificationEvent.getPoolId(), "Shoulda returned the same pool id as provided.");
        final Object expected = poolFailure.getType();
        assertEquals(expected, poolFailureNotificationEvent.getType(), "Shoulda returned the same type as provided.");
    }
}
