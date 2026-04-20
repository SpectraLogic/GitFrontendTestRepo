/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.db.lang.ConfigureSqlLogLevels;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Schema;
import com.spectralogic.util.db.lang.SqlLogLevels;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@Schema( "framework" )
@ConfigureSqlLogLevels( SqlLogLevels.ALL_OPERATIONS_LOGGED_AT_DEBUG_LEVEL )
@UniqueIndexes(
{
    @Unique( Mutex.NAME )
})
public interface Mutex extends DatabasePersistable
{
    String NAME = "name";
    
    String getName();
    
    Mutex setName( final String value );
    
    
    String DATE_CREATED = "dateCreated";

    @DefaultToCurrentDate
    Date getDateCreated();
    
    void setDateCreated( final Date value );
    
    
    String APPLICATION_IDENTIFIER = "applicationIdentifier";
    
    UUID getApplicationIdentifier();
    
    Mutex setApplicationIdentifier( final UUID value );
    
    
    int HEARTBEAT_INTERVAL_IN_SECS = 30;
    int HEARTBEAT_TIMEOUT_IN_SECS = 90;
    String LAST_HEARTBEAT = "lastHeartbeat";

    @DefaultToCurrentDate
    Date getLastHeartbeat();
    
    Mutex setLastHeartbeat( final Date value );
}
