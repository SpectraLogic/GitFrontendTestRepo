/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.List;

import com.spectralogic.s3.common.dao.domain.ds3.JobCreationFailed;
import com.spectralogic.s3.common.platform.notification.domain.payload.JobCreationFailedNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import static com.spectralogic.s3.common.dao.service.ds3.JobCreationFailedService.barCodesAsString;

public final class JobCreationFailedNotificationPayloadGenerator_Test 
{
    @Test
    public void testResponseIsCorrect()
    {
        final List< List< String > > tapeBarCodes = 
                CollectionFactory.toList( CollectionFactory.toList( "a", "b" ) );
        final String userName = "Test";
        final JobCreationFailed failure = BeanFactory.newBean(JobCreationFailed.class)
                .setTapeBarCodes(barCodesAsString(tapeBarCodes))
                .setUserName(userName)
                .setErrorMessage("error!");
        final NotificationPayloadGenerator generator =
                new JobCreationFailedNotificationPayloadGenerator( failure );
        final JobCreationFailedNotificationPayload event = 
                (JobCreationFailedNotificationPayload)
                generator.generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertEquals(
                2,
                event.getTapesMustBeOnlined().getTapesToOnline()[ 0 ].getTapeBarCodes().length,
                "Shoulda returned a sensible response."
                );
    }
}
