/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.target.Ds3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.platform.notification.domain.payload.Ds3TargetFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class Ds3TargetFailureNotificationPayloadGenerator_Test 
{
    @Test
    public void testConstructorNullArgumentNotAllowed()
    {
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        new Ds3TargetFailureNotificationPayloadGenerator( null );
                    }
                } );
    }
    
    
    @Test
    public void testGenerateNotificationEventReturnsRelevantEvent()
    {
        final Ds3TargetFailure targetFailure = BeanFactory.newBean( Ds3TargetFailure.class );
        targetFailure.setDate( new Date() );
        targetFailure.setErrorMessage( "oops, there was an error" );
        targetFailure.setTargetId( UUID.randomUUID() );
        targetFailure.setType( TargetFailureType.NOT_ONLINE );
        final NotificationPayload event = new Ds3TargetFailureNotificationPayloadGenerator( targetFailure )
                .generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertTrue(
                Ds3TargetFailureNotificationPayload.class.isAssignableFrom( event.getClass() ),
                "Shoulda returned a target failure notification event."
               );
        final Ds3TargetFailureNotificationPayload targetFailureNotificationEvent =
                (Ds3TargetFailureNotificationPayload)event;
        final Object expected3 = targetFailure.getDate();
        assertEquals(expected3, targetFailureNotificationEvent.getDate(), "Shoulda returned the same date as provided.");
        final Object expected2 = targetFailure.getErrorMessage();
        assertEquals(expected2, targetFailureNotificationEvent.getErrorMessage(), "Shoulda returned the same error message as provided.");
        final Object expected1 = targetFailure.getTargetId();
        assertEquals(expected1, targetFailureNotificationEvent.getTargetId(), "Shoulda returned the same target id as provided.");
        final Object expected = targetFailure.getType();
        assertEquals(expected, targetFailureNotificationEvent.getType(), "Shoulda returned the same type as provided.");
    }
}
