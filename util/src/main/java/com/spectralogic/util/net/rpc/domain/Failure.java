/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface Failure extends SimpleBeanSafeToProxy
{
    String CODE = "code";
    
    String getCode();
    
    Failure setCode( final String value );
    
    
    String MESSAGE = "message";
    
    /**
     * @return a customer-readable message as to what happened
     */
    String getMessage();
    
    Failure setMessage( final String value );
    
    
    String HTTP_RESPONSE_CODE = "httpResponseCode";
    
    int getHttpResponseCode();
    
    Failure setHttpResponseCode( final int value );
}
