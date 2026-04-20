/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.servlet;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.NamingConventionType;

interface BeanServletParams extends SimpleBeanSafeToProxy
{
    String HTTP_RESPONSE_CODE = "httpResponseCode";
    
    int getHttpResponseCode();
    
    void setHttpResponseCode( final int value );
    
    
    String DATA_TAG = "dataTag";
    
    String getDataTag();
    
    void setDataTag( final String value );
    
    
    String RESPONSE = "response";
    
    Object getResponse();
    
    void setResponse( final Object response );
    
    
    String REQUEST_PATH = "requestPath";
    
    String getRequestPath();
    
    void setRequestPath( final String value );
    
    
    String CONTENT_TYPE = "contentType";
    
    String getContentType();
    
    void setContentType( final String value );
    
    
    String NAMING_CONVENTION = "namingConvention";
    
    NamingConventionType getNamingConvention();
    
    void setNamingConvention( final NamingConventionType value );
}