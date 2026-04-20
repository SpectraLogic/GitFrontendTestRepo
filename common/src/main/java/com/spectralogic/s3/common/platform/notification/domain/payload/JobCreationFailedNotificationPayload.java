/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.notification.domain.NotificationPayload;

/**
 * A job creation failure notification is only generated when there is something the administrator can or
 * should do to correct the cause of the failure so that a subsequent attempt can succeed.  <br><br>
 * 
 * Job creation failures that cannot be corrected via administrative action do not result notifications.
 */
public interface JobCreationFailedNotificationPayload
	extends NotificationPayload, ErrorMessageObservable< JobCreationFailedNotificationPayload >
{
    String TAPES_MUST_BE_ONLINED = "tapesMustBeOnlined";
    
    TapesMustBeOnlined getTapesMustBeOnlined();
    
    void setTapesMustBeOnlined( final TapesMustBeOnlined value );

    String USER_NAME = "userName";

    @References( User.class )
    String getUserName();

    void setUserName( final String value );
}
