/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.notification;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;

    @Indexes( @Index( GenericDaoNotificationRegistration.CREATION_DATE ) )
public interface GenericDaoNotificationRegistration
    extends DatabasePersistable, NotificationRegistrationObservable< GenericDaoNotificationRegistration >
{
    String DAO_TYPE = "daoType";
    
    /**
     * @return the full Java class name of the dao class type that the notification registration is for
     */
    String getDaoType();
    
    GenericDaoNotificationRegistration setDaoType( final String value );
}
