/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.aws;

import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.NamingConventionType;

public enum AWSFailure implements FailureType
{
    /*
     * Please keep sorted by HTTP response code (e.g. 404).  Please keep a newline between different HTTP
     * response codes (e.g. group all 404s together).
     */
    
    BAD_DIGEST( GenericFailure.BAD_REQUEST, null ),
    ENTITY_TOO_SMALL( GenericFailure.BAD_REQUEST, null ),
    ENTITY_TOO_LARGE( GenericFailure.BAD_REQUEST, null ),
    INCOMPLETE_BODY( GenericFailure.BAD_REQUEST, null ),
    INVALID_ARGUMENT( GenericFailure.BAD_REQUEST, null ),
    INVALID_BUCKET_NAME( GenericFailure.BAD_REQUEST, null ),
    INVALID_DIGEST( GenericFailure.BAD_REQUEST, null ),
    INVALID_URI( GenericFailure.BAD_REQUEST, null ),
    MISSING_SECURITY_HEADER( GenericFailure.BAD_REQUEST, null ),
    MULTIPLE_CHECKSUM_HEADERS( GenericFailure.BAD_REQUEST, null ),
    MEDIA_ONLINING_REQUIRED( GenericFailure.CONFLICT, null ),
    
    ACCESS_DENIED( GenericFailure.FORBIDDEN, null ),
    INVALID_ACCESS_KEY_ID( GenericFailure.FORBIDDEN, null ),
    INVALID_SECURITY( GenericFailure.FORBIDDEN, null ),
    REQUEST_TIME_TOO_SKEWED( GenericFailure.FORBIDDEN, null ),
    
    NO_SUCH_BUCKET( GenericFailure.NOT_FOUND, null ),
    NO_SUCH_OBJECT( GenericFailure.NOT_FOUND, "NoSuchKey" ),
    NO_SUCH_UPLOAD( GenericFailure.NOT_FOUND, "NoSuchUpload" ),
    
    BUCKET_ALREADY_EXISTS( GenericFailure.CONFLICT, null ),
    OBJECT_ALREADY_EXISTS( GenericFailure.CONFLICT, null ),
    BUCKET_ALREADY_OWNED_BY_YOU( GenericFailure.CONFLICT, null ),
    BUCKET_NOT_EMPTY( GenericFailure.CONFLICT, null ),
    INVALID_BUCKET_STATE( GenericFailure.CONFLICT, null ),
    INVALID_OBJECT_STATE( GenericFailure.CONFLICT, null ),
    
    SLOW_DOWN_BY_SERVER( GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT, "TemporaryRedirect" ),
    SLOW_DOWN_BY_CLIENT( GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, "SlowDown" ),
    DATABASE_OUT_OF_SPACE( GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, null )
    ;
    
    
    private AWSFailure( final FailureType httpResponseCodeProvider, final String code )
    {
        m_code = ( null == code ) ? 
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( name() )
                : code;
        m_httpResponseCode = httpResponseCodeProvider.getHttpResponseCode();
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
