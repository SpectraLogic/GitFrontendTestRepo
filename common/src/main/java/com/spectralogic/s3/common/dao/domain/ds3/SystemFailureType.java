/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.Severity;
import com.spectralogic.s3.common.dao.domain.shared.SeverityObservable;
import com.spectralogic.util.lang.Validations;

public enum SystemFailureType implements SeverityObservable
{
    RECONCILE_TAPE_ENVIRONMENT_FAILED( Severity.CRITICAL ),
    RECONCILE_POOL_ENVIRONMENT_FAILED( Severity.CRITICAL ),
    CRITICAL_DATA_VERIFICATION_ERROR_REQUIRES_USER_CONFIRMATION( Severity.ALERT ),
    MICROSOFT_AZURE_WRITES_REQUIRE_FEATURE_LICENSE( Severity.ALERT ),
    AWS_S3_WRITES_REQUIRE_FEATURE_LICENSE( Severity.ALERT ),
    DATABASE_RUNNING_OUT_OF_SPACE( Severity.CRITICAL )
    ;
    
    
    private SystemFailureType( final Severity severity )
    {
        m_severity = severity;
        Validations.verifyNotNull( "Severity", m_severity );
    }
    
    
    public Severity getSeverity()
    {
        return m_severity;
    }
    
    
    private final Severity m_severity;
}
