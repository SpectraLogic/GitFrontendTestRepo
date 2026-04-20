/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;

@CustomMarshaledTypeName( "Error" )
public interface HttpErrorResultApiBean extends SimpleBeanSafeToProxy
{
    String CODE = "code";
    
    String getCode();
    
    HttpErrorResultApiBean setCode( final String value );
    
    
    String HTTP_ERROR_CODE = "httpErrorCode";
    
    int getHttpErrorCode();
    
    HttpErrorResultApiBean setHttpErrorCode( final int value );
    
    
    String MESSAGE = "message";
    
    String getMessage();
    
    HttpErrorResultApiBean setMessage( final String message );
    
    
    String RESOURCE = "resource";
    
    String getResource();
    
    void setResource( final String value );
    
    
    String RESOURCE_ID = "resourceId";
    
    long getResourceId();
    
    void setResourceId( final long value );
}
