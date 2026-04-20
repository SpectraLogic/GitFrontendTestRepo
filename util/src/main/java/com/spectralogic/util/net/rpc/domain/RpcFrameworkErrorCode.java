/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.domain;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.lang.NamingConventionType;

public enum RpcFrameworkErrorCode implements FailureType
{
    RESOURCE_TYPE_NOT_FOUND( HttpServletResponse.SC_NOT_FOUND ),
    RESOURCE_INSTANCE_NOT_FOUND( HttpServletResponse.SC_NOT_FOUND ),
    ;
    
    
    private RpcFrameworkErrorCode( final int httpResponseCode )
    {
        m_httpResponseCode = httpResponseCode;
        m_code = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( name() );
    }
    
    
    public int getHttpResponseCode()
    {
        return m_httpResponseCode;
    }
    
    
    public String getCode()
    {
        return m_code;
    }
    
    
    private final String m_code;
    private final int m_httpResponseCode;
}
