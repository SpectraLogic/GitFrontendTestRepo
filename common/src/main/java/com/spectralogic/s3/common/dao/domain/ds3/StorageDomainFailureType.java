/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.Severity;
import com.spectralogic.util.lang.Validations;

public enum StorageDomainFailureType
{
    ILLEGAL_EJECTION_OCCURRED( Severity.WARNING ),
    MEMBER_BECAME_READ_ONLY( Severity.WARNING ),
    WRITES_STALLED_DUE_TO_NO_FREE_MEDIA_REMAINING( Severity.ALERT ),
    ;
    
    
    private StorageDomainFailureType( final Severity severity )
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
