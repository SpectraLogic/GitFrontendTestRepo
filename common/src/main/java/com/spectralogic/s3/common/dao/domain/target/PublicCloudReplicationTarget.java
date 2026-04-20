/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import java.util.Date;

import com.spectralogic.s3.common.dao.domain.ds3.CloudNamingMode;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultStringValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.MustMatchRegularExpression;

public interface PublicCloudReplicationTarget< T > extends ReplicationTarget< T >
{
    String HTTPS = "https";
    
    @DefaultBooleanValue( true )
    boolean isHttps();
    
    T setHttps( final boolean value );
    
    
    String CLOUD_BUCKET_PREFIX = "cloudBucketPrefix";
    
    @DefaultStringValue( "" )
    // Gross validation based on union of AWS/Azure bucket naming restrictions
    @MustMatchRegularExpression( "^([a-z\\d]([a-z\\d]|(\\.(?!(\\.|-)))|(-(?!\\.))){0,62}){0,1}$" )
    String getCloudBucketPrefix();
    
    T setCloudBucketPrefix( final String value );
    

    String CLOUD_BUCKET_SUFFIX= "cloudBucketSuffix";

    @DefaultStringValue( "" )
    // Gross validation based on union of AWS/Azure bucket naming restrictions
    @MustMatchRegularExpression( "^(([a-z\\d]|(\\.(?!(\\.|-)))|(-(?!\\.))){0,62}[a-z\\d\\.]){0,1}$" )
    String getCloudBucketSuffix();
    
    T setCloudBucketSuffix( final String value );
    
    
    String LAST_FULLY_VERIFIED = "lastFullyVerified";
    
    @Optional
    Date getLastFullyVerified();
    
    T setLastFullyVerified( final Date value );
    
    
    String AUTO_VERIFY_FREQUENCY_IN_DAYS = "autoVerifyFrequencyInDays";
    
    @Optional
    Integer getAutoVerifyFrequencyInDays();
    
    T setAutoVerifyFrequencyInDays( final Integer value );
    
    
    String NAMING_MODE = "namingMode";
    
    @DefaultEnumValue("BLACK_PEARL")
    CloudNamingMode getNamingMode();
    
    T setNamingMode( final CloudNamingMode value );
}
