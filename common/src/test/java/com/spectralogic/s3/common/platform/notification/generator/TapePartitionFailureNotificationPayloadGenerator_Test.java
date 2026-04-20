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

import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.platform.notification.domain.payload.TapePartitionFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class TapePartitionFailureNotificationPayloadGenerator_Test 
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
                        new TapePartitionFailureNotificationPayloadGenerator( null );
                    }
                } );
    }
    
    
    @Test
    public void testGenerateNotificationEventReturnsRelevantEvent()
    {
        final TapePartitionFailure tapePartitionFailure = BeanFactory.newBean( TapePartitionFailure.class );
        tapePartitionFailure.setDate( new Date() );
        tapePartitionFailure.setErrorMessage( "oops, there was an error" );
        tapePartitionFailure.setPartitionId( UUID.randomUUID() );
        tapePartitionFailure.setType( TapePartitionFailureType.values()[ 0 ] );
        final NotificationPayload event =
                new TapePartitionFailureNotificationPayloadGenerator( tapePartitionFailure )
                        .generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertTrue(
                TapePartitionFailureNotificationPayload.class.isAssignableFrom( event.getClass() ),
                "Shoulda returned a tape partition failure notification event."
                );
        final TapePartitionFailureNotificationPayload tapePartitionFailureNotificationEvent =
                (TapePartitionFailureNotificationPayload)event;
        final Object expected3 = tapePartitionFailure.getDate();
        assertEquals(expected3, tapePartitionFailureNotificationEvent.getDate(), "Shoulda returned the same date as provided.");
        final Object expected2 = tapePartitionFailure.getErrorMessage();
        assertEquals(expected2, tapePartitionFailureNotificationEvent.getErrorMessage(), "Shoulda returned the same error message as provided.");
        final Object expected1 = tapePartitionFailure.getPartitionId();
        assertEquals(expected1, tapePartitionFailureNotificationEvent.getPartitionId(), "Shoulda returned the same partition id as provided.");
        final Object expected = tapePartitionFailure.getType();
        assertEquals(expected, tapePartitionFailureNotificationEvent.getType(), "Shoulda returned the same type as provided.");
    }
}
