/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain.bean;

import java.util.Date;

import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.http.HttpResponseFormatType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;

public interface HttpNotificationRegistration< T extends HttpNotificationRegistration< ? > > 
    extends SimpleBeanSafeToProxy
{
    String NOTIFICATION_END_POINT = "notificationEndPoint";
    
    /**
     * @return the URI that should be used when sending an HTTP notification
     */
    String getNotificationEndPoint();
    
    T setNotificationEndPoint( final String value );
    
    
    String NOTIFICATION_HTTP_METHOD = "notificationHttpMethod";
    
    /**
     * @return the HTTP method that should be used when sending an HTTP notification
     */
    @DefaultEnumValue( "POST" )
    RequestType getNotificationHttpMethod();
    
    T setNotificationHttpMethod( final RequestType value );
    
    
    String NAMING_CONVENTION = "namingConvention";
    
    /**
     * @return the naming convention to use in the payload when sending an HTTP notification
     */
    @DefaultEnumValue( "CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE" )
    NamingConventionType getNamingConvention();
    
    T setNamingConvention( final NamingConventionType value );
    
    
    String FORMAT = "format";
    
    /**
     * @return the format to use for the payload when sending an HTTP notification
     */
    @DefaultEnumValue( "DEFAULT" )
    HttpResponseFormatType getFormat();
    
    T setFormat( final HttpResponseFormatType value );
    
    
    String CREATION_DATE = "creationDate";

    @DefaultToCurrentDate
    @SortBy
    Date getCreationDate();
    
    T setCreationDate( final Date value );
    
    
    String NUMBER_OF_FAILURES_SINCE_LAST_SUCCESS = "numberOfFailuresSinceLastSuccess";
    
    int getNumberOfFailuresSinceLastSuccess();
    
    T setNumberOfFailuresSinceLastSuccess( final int value );
    
    
    String LAST_FAILURE = "lastFailure";
    
    @Optional
    String getLastFailure();
    
    void setLastFailure( final String value );
    
    
    String LAST_HTTP_RESPONSE_CODE = "lastHttpResponseCode";
    
    @Optional
    Integer getLastHttpResponseCode();
    
    T setLastHttpResponseCode( final Integer value );
    
    
    String LAST_NOTIFICATION = "lastNotification";
    
    @Optional
    Date getLastNotification();
    
    T setLastNotification( final Date value );
}
