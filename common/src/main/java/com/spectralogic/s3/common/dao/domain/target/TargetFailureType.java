/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import com.spectralogic.s3.common.dao.domain.shared.Severity;
import com.spectralogic.s3.common.dao.domain.shared.SeverityObservable;
import com.spectralogic.util.lang.Validations;

public enum TargetFailureType implements SeverityObservable
{
    IMPORT_FAILED( Severity.ALERT ),
    IMPORT_INCOMPLETE ( Severity.WARNING ),
    NOT_ONLINE( Severity.WARNING ),
    WRITE_FAILED( Severity.WARNING ),
    WRITE_INITIATE_FAILED( Severity.WARNING ),
    READ_FAILED( Severity.WARNING ),
    READ_INITIATE_FAILED( Severity.WARNING ),
    VERIFY_FAILED( Severity.WARNING ),
    VERIFY_COMPLETE( Severity.SUCCESS )
    ;
    
    
    private TargetFailureType( final Severity severity )
    {
        m_severity = severity;
        Validations.verifyNotNull( "Severity", severity );
    }
    
    
    public Severity getSeverity()
    {
        return m_severity;
    }
    
    
    private final Severity m_severity;
}
