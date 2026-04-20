/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.platform.notification.domain.payload.JobCreatedNotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class JobCreatedNotificationPayloadGenerator_Test 
{
    @Test
    public void testConstructorNullJobIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new JobCreatedNotificationPayloadGenerator( null );
                }
            } );
    }
    

    @Test
    public void testHappyConstruction()
    {
        new JobCreatedNotificationPayloadGenerator( UUID.randomUUID() );
    }
    
    
    @Test
    public void testResponseIsCorrect()
    {
        final UUID id = UUID.randomUUID();
        final NotificationPayloadGenerator generator = new JobCreatedNotificationPayloadGenerator( id );
        final JobCreatedNotificationPayload event = 
                (JobCreatedNotificationPayload)generator.generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertEquals(
                id,
                event.getJobId(),
                "Shoulda returned a sensible response."
                 );
    }
}
