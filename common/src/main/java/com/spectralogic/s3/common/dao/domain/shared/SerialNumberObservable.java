/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import com.spectralogic.util.bean.lang.SortBy;

public interface SerialNumberObservable< T >
{
    String SERIAL_NUMBER = "serialNumber";
    
    @SortBy( 20 )
    String getSerialNumber();
    
    T setSerialNumber( final String value );
}
