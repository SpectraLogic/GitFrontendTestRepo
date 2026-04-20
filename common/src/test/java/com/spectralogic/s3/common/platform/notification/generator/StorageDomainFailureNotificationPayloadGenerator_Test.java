/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType;
import com.spectralogic.s3.common.platform.notification.domain.payload.StorageDomainFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class StorageDomainFailureNotificationPayloadGenerator_Test 
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
                        new StorageDomainFailureNotificationPayloadGenerator( null );
                    }
                } );
    }
    
    
    @Test
    public void testGenerateNotificationEventReturnsRelevantEvent()
    {
        final StorageDomainFailure storageDomainFailure = BeanFactory.newBean( StorageDomainFailure.class );
        storageDomainFailure.setDate( new Date() );
        storageDomainFailure.setErrorMessage( "oops, there was an error" );
        storageDomainFailure.setStorageDomainId( UUID.randomUUID() );
        storageDomainFailure.setType( StorageDomainFailureType.values()[ 0 ] );
        final NotificationPayload event =
                new StorageDomainFailureNotificationPayloadGenerator( storageDomainFailure )
                        .generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertTrue(
                StorageDomainFailureNotificationPayload.class.isAssignableFrom( event.getClass() ),
                "Shoulda returned a tape partition failure notification event."
                );
        final StorageDomainFailureNotificationPayload storageDomainFailureNotificationEvent =
                (StorageDomainFailureNotificationPayload)event;
        final Object expected3 = storageDomainFailure.getDate();
        assertEquals(expected3, storageDomainFailureNotificationEvent.getDate(), "Shoulda returned the same date as provided.");
        final Object expected2 = storageDomainFailure.getErrorMessage();
        assertEquals(expected2, storageDomainFailureNotificationEvent.getErrorMessage(), "Shoulda returned the same error message as provided.");
        final Object expected1 = storageDomainFailure.getStorageDomainId();
        assertEquals(expected1, storageDomainFailureNotificationEvent.getStorageDomainId(), "Shoulda returned the same partition id as provided.");
        final Object expected = storageDomainFailure.getType();
        assertEquals(expected, storageDomainFailureNotificationEvent.getType(), "Shoulda returned the same type as provided.");
    }
}
