/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.notification.NotificationRegistrationObservable;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.http.HttpResponseFormatType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class NotificationRegistrationTestUtil
{
    public static < T extends NotificationRegistrationObservable< ? > & DatabasePersistable >
            List< T > createRegistrations( final Class< T > notificationType, final UUID userId )
    {
        final T registration1 = BeanFactory.newBean( notificationType );
        registration1.setUserId( userId );
        registration1.setFormat( HttpResponseFormatType.DEFAULT );
        registration1.setNamingConvention( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE );
        registration1.setNotificationHttpMethod( RequestType.POST );
        registration1.setNotificationEndPoint( "a" );

        // NotificationRegistrationObservable is sorted by create date, so make
        // sure there's a slight difference in create date so the tests pass
        // reliably.
        TestUtil.sleep( 1 );

        final T registration2 = BeanFactory.newBean( notificationType );
        registration2.setUserId( userId );
        registration2.setFormat( HttpResponseFormatType.DEFAULT );
        registration2.setNamingConvention( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE );
        registration2.setNotificationHttpMethod( RequestType.POST );
        registration2.setNotificationEndPoint( "b" );
        return CollectionFactory.toList( registration1, registration2 );
    }


    public static < T extends DatabasePersistable > void saveRegistrations(
            final DatabaseSupport databaseSupport,
            final List< T > registrations )
    {
        final DataManager dataManager = databaseSupport.getDataManager();
        for ( final T registration : registrations )
        {
            dataManager.createBean( registration );
        }
    }
}
