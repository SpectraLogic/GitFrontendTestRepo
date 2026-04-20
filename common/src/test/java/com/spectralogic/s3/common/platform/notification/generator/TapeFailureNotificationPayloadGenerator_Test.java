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

import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.platform.notification.domain.payload.TapeFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class TapeFailureNotificationPayloadGenerator_Test 
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
                        new TapeFailureNotificationPayloadGenerator( null );
                    }
                } );
    }
    
    
    @Test
    public void testGenerateNotificationEventReturnsRelevantEvent()
    {
        final TapeFailure tapeFailure = BeanFactory.newBean( TapeFailure.class );
        tapeFailure.setDate( new Date() );
        tapeFailure.setErrorMessage( "oops, there was an error" );
        tapeFailure.setTapeDriveId( UUID.randomUUID() );
        tapeFailure.setTapeId( UUID.randomUUID() );
        tapeFailure.setType( TapeFailureType.INSPECT_FAILED );
        final NotificationPayload event = new TapeFailureNotificationPayloadGenerator( tapeFailure )
                .generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertTrue(
                TapeFailureNotificationPayload.class.isAssignableFrom( event.getClass() ),
                "Shoulda returned a tape failure notification event."
               );
        final TapeFailureNotificationPayload tapeFailureNotificationEvent =
                (TapeFailureNotificationPayload)event;
        final Object expected4 = tapeFailure.getDate();
        assertEquals(expected4, tapeFailureNotificationEvent.getDate(), "Shoulda returned the same date as provided.");
        final Object expected3 = tapeFailure.getErrorMessage();
        assertEquals(expected3, tapeFailureNotificationEvent.getErrorMessage(), "Shoulda returned the same error message as provided.");
        final Object expected2 = tapeFailure.getTapeDriveId();
        assertEquals(expected2, tapeFailureNotificationEvent.getTapeDriveId(), "Shoulda returned the same tape drive id as provided.");
        final Object expected1 = tapeFailure.getTapeId();
        assertEquals(expected1, tapeFailureNotificationEvent.getTapeId(), "Shoulda returned the same tape id as provided.");
        final Object expected = tapeFailure.getType();
        assertEquals(expected, tapeFailureNotificationEvent.getType(), "Shoulda returned the same type as provided.");
    }
}
