/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes( @Unique( NameObservable.NAME ) )
public interface Ds3Target extends ReplicationTarget< Ds3Target >, DatabasePersistable
{
    /**
     * @return the instance id of the DS3 target
     */
    UUID getId();
    
    
    String DATA_PATH_END_POINT = "dataPathEndPoint";
    
    /**
     * @return the remote DS3 replication target's data path IP address or DNS name, which must be accessible
     * from this appliance's data path
     */
    String getDataPathEndPoint();
    
    Ds3Target setDataPathEndPoint( final String dataPath );
    
    
    String DATA_PATH_PORT = "dataPathPort";
    
    @Optional
    Integer getDataPathPort();
    
    Ds3Target setDataPathPort( final Integer value );
    
    
    String DATA_PATH_HTTPS = "dataPathHttps";
    
    @DefaultBooleanValue( true )
    boolean isDataPathHttps();
    
    Ds3Target setDataPathHttps( final boolean value );
    
    
    String DATA_PATH_PROXY = "dataPathProxy";
    
    @Optional
    String getDataPathProxy();
    
    Ds3Target setDataPathProxy( final String value );
    
    
    String DATA_PATH_VERIFY_CERTIFICATE = "dataPathVerifyCertificate";
    
    @DefaultBooleanValue( true )
    boolean isDataPathVerifyCertificate();
    
    Ds3Target setDataPathVerifyCertificate( final boolean value );
    
    
    String ADMIN_AUTH_ID = "adminAuthId";
    
    String getAdminAuthId();
    
    Ds3Target setAdminAuthId( final String value );
    
    
    String ADMIN_SECRET_KEY = "adminSecretKey";
    
    @Secret
    String getAdminSecretKey();
    
    Ds3Target setAdminSecretKey( final String value );
    
    
    String ACCESS_CONTROL_REPLICATION = "accessControlReplication";
    
    @DefaultEnumValue( "NONE" )
    Ds3TargetAccessControlReplication getAccessControlReplication();
    
    Ds3Target setAccessControlReplication( final Ds3TargetAccessControlReplication value );
    
    
    String REPLICATED_USER_DEFAULT_DATA_POLICY = "replicatedUserDefaultDataPolicy";
    
    /**
     * @return the default data policy to set for any users created on the target due to access control 
     * replication
     */
    @Optional
    String getReplicatedUserDefaultDataPolicy();
    
    Ds3Target setReplicatedUserDefaultDataPolicy( final String value );
}
