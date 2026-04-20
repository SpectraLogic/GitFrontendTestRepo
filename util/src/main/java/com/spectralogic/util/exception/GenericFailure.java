/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.exception;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.util.lang.NamingConventionType;

public enum GenericFailure implements FailureType
{
    /*
     * Please keep sorted by HTTP response code (e.g. 404).  Please keep a newline between major divisions
     * (e.g. 2xx vs 3xx codes).
     */
    FORCE_FLAG_REQUIRED_OK( HttpServletResponse.SC_NO_CONTENT, null ),

    MULTIPLE_RESULTS_FOUND( HttpServletResponse.SC_MULTIPLE_CHOICES, null ),
    RETRY_WITH_SYNCHRONOUS_WAIT( HttpServletResponse.SC_TEMPORARY_REDIRECT, null ),

    FORCE_FLAG_REQUIRED( HttpServletResponse.SC_BAD_REQUEST, null ),
    BAD_REQUEST( HttpServletResponse.SC_BAD_REQUEST, null ),
    FEATURE_KEY_REQUIRED( HttpServletResponse.SC_PAYMENT_REQUIRED, null ),
    FORBIDDEN( HttpServletResponse.SC_FORBIDDEN, null ),
    NOT_FOUND( HttpServletResponse.SC_NOT_FOUND, null ),
    CANNOT_LOCK_AT_THIS_TIME( HttpServletResponse.SC_CONFLICT, null ),
    CONFLICT( HttpServletResponse.SC_CONFLICT, null ),
    GONE( HttpServletResponse.SC_GONE, null ),
    CONTENT_LENGTH_REQUIRED( HttpServletResponse.SC_LENGTH_REQUIRED, null ),
    REQUEST_ENTITY_TOO_LARGE( HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE, null ),

    INTERNAL_ERROR( HttpServletResponse.SC_INTERNAL_SERVER_ERROR, null ),
    RETRY_WITH_ASYNCHRONOUS_WAIT( HttpServletResponse.SC_SERVICE_UNAVAILABLE, null ),
    ;
    
    
    private GenericFailure( final int httpResponseCode, final String code )
    {
        m_httpResponseCode = httpResponseCode;
        m_code = ( null == code ) ? 
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( name() )
                : code;
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