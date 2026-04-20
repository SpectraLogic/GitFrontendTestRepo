/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.JobCreationFailed;
import com.spectralogic.s3.common.platform.notification.domain.payload.JobCreationFailedNotificationPayload;
import com.spectralogic.s3.common.platform.notification.domain.payload.SetOfTapeBarCodes;
import com.spectralogic.s3.common.platform.notification.domain.payload.TapesMustBeOnlined;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

import static com.spectralogic.s3.common.dao.service.ds3.JobCreationFailedService.parseBarCodes;
import static org.apache.commons.lang3.StringUtils.split;

public final class JobCreationFailedNotificationPayloadGenerator
    implements NotificationPayloadGenerator
{
    public JobCreationFailedNotificationPayloadGenerator(final JobCreationFailed failure)
    {
        m_failure = failure;
    }

    
    public NotificationPayload generateNotificationPayload()
    {
        final List<List<String>> tapeBarCodes = parseBarCodes(m_failure.getTapeBarCodes());
        final JobCreationFailedNotificationPayload retval = 
                BeanFactory.newBean( JobCreationFailedNotificationPayload.class );
        final List< SetOfTapeBarCodes > lists = new ArrayList<>();
        for ( final Collection < String > collection : tapeBarCodes )
        {
            final SetOfTapeBarCodes set = BeanFactory.newBean( SetOfTapeBarCodes.class );
            set.setTapeBarCodes( CollectionFactory.toArray( String.class, collection ) );
            lists.add( set );
        }
        
        final TapesMustBeOnlined tmbo = BeanFactory.newBean( TapesMustBeOnlined.class );
        tmbo.setTapesToOnline( CollectionFactory.toArray( SetOfTapeBarCodes.class, lists ) );

        retval.setTapesMustBeOnlined( tmbo );
        retval.setUserName( m_failure.getUserName() );
        retval.setErrorMessage( m_failure.getErrorMessage() );
        return retval;
    }

    private final JobCreationFailed m_failure;
}
