/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.notification;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public interface NotificationRegistrationObservable< T extends HttpNotificationRegistration< ? > >
    extends SimpleBeanSafeToProxy, UserIdObservable< T >, HttpNotificationRegistration< T >
{
    @References( User.class )
    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getUserId();
}
