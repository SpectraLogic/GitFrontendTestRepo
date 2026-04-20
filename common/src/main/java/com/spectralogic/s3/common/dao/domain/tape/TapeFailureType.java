/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import com.spectralogic.s3.common.dao.domain.shared.Severity;
import com.spectralogic.util.lang.Validations;

public enum TapeFailureType
{
    BAR_CODE_CHANGED( Severity.CRITICAL ),
    BAR_CODE_DUPLICATE( Severity.CRITICAL ),
    BLOB_READ_FAILED( Severity.WARNING ),
    CLEANING_TAPE_EXPIRED( Severity.WARNING ),
    DATA_CHECKPOINT_FAILURE( Severity.CRITICAL ),
    DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY( Severity.WARNING ),
    DATA_CHECKPOINT_MISSING( Severity.CRITICAL ),
    DELAYED_OWNERSHIP_FAILURE( Severity.WARNING ),
    DRIVE_CLEAN_FAILED( Severity.WARNING ),
    DRIVE_CLEANED( Severity.SUCCESS ),
    DRIVE_TEST_FAILED( Severity.WARNING ),
    DRIVE_TEST_FAILED_ALL_WRITES_TOO_SLOW( Severity.WARNING ),
    DRIVE_TEST_FAILED_FORWARD_WRITES_TOO_SLOW( Severity.WARNING ),
    DRIVE_TEST_FAILED_REVERSE_WRITES_TOO_SLOW( Severity.WARNING ),
    DRIVE_TEST_SUCCEEDED( Severity.SUCCESS ),
    ENCRYPTION_ERROR ( Severity.CRITICAL ),
    FORMAT_FAILED( Severity.WARNING ),
    GET_TAPE_INFORMATION_FAILED( Severity.WARNING ),
    HARDWARE_ERROR ( Severity.CRITICAL ),
    IMPORT_FAILED( Severity.ALERT ),
    IMPORT_INCOMPLETE ( Severity.WARNING ),
    IMPORT_FAILED_DUE_TO_TAKE_OWNERSHIP_FAILURE( Severity.ALERT ),
    IMPORT_FAILED_DUE_TO_DATA_INTEGRITY( Severity.ALERT ),
    INCOMPATIBLE( Severity.INFO ),
    INSPECT_FAILED( Severity.WARNING ),
    MOVE_FAILED( Severity.WARNING ),
    QUIESCING_DRIVE( Severity.WARNING ),
    READ_FAILED( Severity.WARNING ),
    REIMPORT_REQUIRED( Severity.ALERT ),
    SERIAL_NUMBER_MISMATCH( Severity.WARNING ),
    SINGLE_PARTITION( Severity.WARNING ),
    VERIFY_FAILED( Severity.WARNING ),
    WRITE_FAILED( Severity.WARNING ),
    WRITE_SOURCE_FAILED( Severity.WARNING )
    ;
    
    
    private TapeFailureType( final Severity severity )
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
