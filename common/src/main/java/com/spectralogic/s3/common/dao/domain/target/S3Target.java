/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import com.spectralogic.s3.common.dao.domain.ds3.S3Region;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

@Indexes( @Index( S3Target.NAME ) )
public interface S3Target extends DataOfflineablePublicCloudReplicationTarget< S3Target >, DatabasePersistable
{
    String REGION = "region";

    @Optional
    @DefaultEnumValue( "US_WEST_2" )
    S3Region getRegion();
    
    S3Target setRegion( final S3Region value );
    
    
    String DATA_PATH_END_POINT = "dataPathEndPoint";
    
    /**
     * @return the remote S3 replication target's data path IP address or DNS name, which must be accessible
     * from this appliance's data path (if null, will connect to the AWS S3 cloud hosted by Amazon)
     */
    @Optional
    String getDataPathEndPoint();
    
    S3Target setDataPathEndPoint( final String dataPath );
    
    
    String PROXY_HOST = "proxyHost";
    
    @Optional
    String getProxyHost();
    
    S3Target setProxyHost( final String value );
    
    
    String PROXY_PORT = "proxyPort";

    @Optional
    Integer getProxyPort();
    
    S3Target setProxyPort( final Integer value );
    
    
    String PROXY_USERNAME = "proxyUsername";

    @Optional
    String getProxyUsername();
    
    S3Target setProxyUsername( final String value );
    
    
    String PROXY_PASSWORD = "proxyPassword";

    @Optional
    String getProxyPassword();
    
    S3Target setProxyPassword( final String value );
    
    
    String PROXY_DOMAIN = "proxyDomain";

    @Optional
    String getProxyDomain();
    
    S3Target setProxyDomain( final String value );
    
    
    String ACCESS_KEY = "accessKey";
    
    String getAccessKey();
    
    S3Target setAccessKey( final String value );
    
    
    String SECRET_KEY = "secretKey";
    
    @Secret
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    String getSecretKey();
    
    S3Target setSecretKey( final String value );


    String RESTRICTED_ACCESS = "restrictedAccess";

    @DefaultBooleanValue( false )
    boolean isRestrictedAccess();

    S3Target setRestrictedAccess( final boolean value );
}
