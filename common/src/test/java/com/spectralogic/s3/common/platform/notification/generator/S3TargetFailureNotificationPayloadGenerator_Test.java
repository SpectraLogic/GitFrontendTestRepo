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

import com.spectralogic.s3.common.dao.domain.target.S3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.platform.notification.domain.payload.S3TargetFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class S3TargetFailureNotificationPayloadGenerator_Test 
{

    private void assertTrueMod(final String message,  final boolean actual) {
        assertTrue(actual, message);
    }

    private void assertFalseMod(final String message,  final boolean actual) {
        assertFalse(actual,  message);
    }

    private void assertNullMod(final String message,  final Object actual) {
        assertNull(actual,  message);
    }

    private void assertNotNullMod(final String message,  final Object actual) {
        assertNotNull(actual,  message);
    }

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
                        new S3TargetFailureNotificationPayloadGenerator( null );
                    }
                } );
    }
    
    
    @Test
    public void testGenerateNotificationEventReturnsRelevantEvent()
    {
        final S3TargetFailure targetFailure = BeanFactory.newBean( S3TargetFailure.class );
        targetFailure.setDate( new Date() );
        targetFailure.setErrorMessage( "oops, there was an error" );
        targetFailure.setTargetId( UUID.randomUUID() );
        targetFailure.setType( TargetFailureType.NOT_ONLINE );
        final NotificationPayload event = new S3TargetFailureNotificationPayloadGenerator( targetFailure )
                .generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertTrue(
                S3TargetFailureNotificationPayload.class.isAssignableFrom( event.getClass() ),
                "Shoulda returned a target failure notification event."
                 );
        final S3TargetFailureNotificationPayload targetFailureNotificationEvent =
                (S3TargetFailureNotificationPayload)event;
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
