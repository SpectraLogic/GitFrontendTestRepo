/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.util.bean.lang.DefaultStringValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.ConfigureSqlLogLevels;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.SqlLogLevels;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A single Spectra appliance (not a tape library, partition, or other tape entity).
 */
@ConfigureSqlLogLevels( SqlLogLevels.UPDATES_LOGGED_AT_DEBUG_LEVEL )
@UniqueIndexes(
{
    @Unique( NameObservable.NAME ),
    @Unique( SerialNumberObservable.SERIAL_NUMBER )
})
public interface Node extends DatabasePersistable, NameObservable< Node >, SerialNumberObservable< Node >
{
    String LAST_HEARTBEAT = "lastHeartbeat";
    int INTERVAL_BETWEEN_HEARTBEATS_IN_SECS = 25;
    int MAX_DURATION_SINCE_LAST_HEARTBEAT_TO_CONSIDER_ALIVE_IN_SECS = 60;

    @DefaultToCurrentDate
    Date getLastHeartbeat();
    
    Node setLastHeartbeat( final Date value );
    
    
    String DNS_NAME = "dnsName";
    
    @Optional
    String getDnsName();
    
    Node setDnsName( final String value );
    
    
    String DATA_PATH_IP_ADDRESS = "dataPathIpAddress";
    
    @DefaultStringValue( "NOT_INITIALIZED_YET" )
    String getDataPathIpAddress();
    
    Node setDataPathIpAddress( final String value );
    
    
    String DATA_PATH_HTTP_PORT = "dataPathHttpPort";
    
    @Optional
    Integer getDataPathHttpPort();
    
    Node setDataPathHttpPort( final Integer value );
    
    
    String DATA_PATH_HTTPS_PORT = "dataPathHttpsPort";
    
    @Optional
    Integer getDataPathHttpsPort();
    
    Node setDataPathHttpsPort( final Integer value );
}
