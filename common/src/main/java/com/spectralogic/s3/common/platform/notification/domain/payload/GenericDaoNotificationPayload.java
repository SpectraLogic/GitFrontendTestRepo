/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import java.util.UUID;

import com.spectralogic.util.db.lang.SqlOperation;
import com.spectralogic.util.notification.domain.NotificationPayload;

public interface GenericDaoNotificationPayload extends NotificationPayload
{
    String DAO_TYPE = "daoType";
    
    String getDaoType();
    
    void setDaoType( final String value );
    
    
    String SQL_OPERATION = "sqlOperation";
    
    SqlOperation getSqlOperation();
    
    void setSqlOperation( final SqlOperation value );
    
    
    String IDS = "ids";
    
    UUID [] getIds();
    
    void setIds( final UUID [] value );
}
