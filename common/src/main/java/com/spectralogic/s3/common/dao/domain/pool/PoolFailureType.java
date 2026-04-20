/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.pool;

import com.spectralogic.s3.common.dao.domain.shared.Severity;
import com.spectralogic.util.lang.Validations;

public enum PoolFailureType
{
    BLOB_READ_FAILED( Severity.WARNING ),
    DATA_CHECKPOINT_FAILURE( Severity.CRITICAL ),
    DATA_CHECKPOINT_MISSING( Severity.CRITICAL ),
    FORMAT_FAILED( Severity.WARNING ),
    IMPORT_FAILED( Severity.ALERT ),
    IMPORT_INCOMPLETE ( Severity.WARNING ),
    IMPORT_FAILED_DUE_TO_TAKE_OWNERSHIP_FAILURE( Severity.ALERT ),
    IMPORT_FAILED_DUE_TO_DATA_INTEGRITY( Severity.ALERT ),
    INSPECT_FAILED( Severity.WARNING ),
    QUIESCED( Severity.ALERT ),
    READ_FAILED( Severity.WARNING ),
    VERIFY_FAILED( Severity.WARNING ),
    WRITE_FAILED( Severity.WARNING ),
    ;
    
    
    private PoolFailureType( final Severity severity )
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
