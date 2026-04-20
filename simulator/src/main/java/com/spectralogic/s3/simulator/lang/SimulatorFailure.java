/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.lang;

import com.spectralogic.s3.common.rpc.tape.TapeResourceFailureCode;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.lang.NamingConventionType;

public enum SimulatorFailure implements FailureType
{
    TAPE_ENVIRONMENT_CHANGED( 500, TapeResourceFailureCode.TAPE_ENVIRONMENT_CHANGED.toString() ),
    ;
    
    
    private SimulatorFailure( final int httpResponseCode, final String code )
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
