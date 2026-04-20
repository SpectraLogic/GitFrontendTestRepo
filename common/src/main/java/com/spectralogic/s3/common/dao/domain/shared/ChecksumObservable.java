/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.security.ChecksumType;

public interface ChecksumObservable< T > extends SimpleBeanSafeToProxy
{
    String CHECKSUM = "checksum";
    
    /**
     * @return the Base64-encoded checksum
     */
    @Optional
    String getChecksum();
    
    T setChecksum( final String value );
    
    
    String CHECKSUM_TYPE = "checksumType";
    
    @Optional
    ChecksumType getChecksumType();
    
    T setChecksumType( final ChecksumType value );
    
    
    /**
     * In some cases, we must report a checksum type and value, but need to indicate that we didn't actually
     * compute the checksum since it's unsupported to do so for the given use case.  In this case, the 
     * checksum value shall be the value below, and the checksum type can be anything.
     */
    public String CHECKSUM_VALUE_NOT_COMPUTED = "NOT_COMPUTED";
}
