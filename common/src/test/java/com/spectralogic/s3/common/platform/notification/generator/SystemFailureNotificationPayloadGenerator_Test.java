/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.Date;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.platform.notification.domain.payload.SystemFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class SystemFailureNotificationPayloadGenerator_Test 
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
                        new SystemFailureNotificationPayloadGenerator( null );
                    }
                } );
    }
    
    
    @Test
    public void testGenerateNotificationEventReturnsRelevantEvent()
    {
        final SystemFailure failure = BeanFactory.newBean( SystemFailure.class );
        failure.setDate( new Date() );
        failure.setErrorMessage( "oops, there was an error" );
        failure.setType( SystemFailureType.values()[ 0 ] );
        final NotificationPayload event =
                new SystemFailureNotificationPayloadGenerator( failure )
                        .generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertTrue(
                SystemFailureNotificationPayload.class.isAssignableFrom( event.getClass() ),
                "Shoulda returned a tape partition failure notification event."
                );
        final SystemFailureNotificationPayload storageDomainFailureNotificationEvent =
                (SystemFailureNotificationPayload)event;
        final Object expected2 = failure.getDate();
        assertEquals(expected2, storageDomainFailureNotificationEvent.getDate(), "Shoulda returned the same date as provided.");
        final Object expected1 = failure.getErrorMessage();
        assertEquals(expected1, storageDomainFailureNotificationEvent.getErrorMessage(), "Shoulda returned the same error message as provided.");
        final Object expected = failure.getType();
        assertEquals(expected, storageDomainFailureNotificationEvent.getType(), "Shoulda returned the same type as provided.");
    }
}
