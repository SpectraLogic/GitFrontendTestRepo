/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.domain.tape;

import com.spectralogic.s3.common.dao.domain.shared.Severity;
import com.spectralogic.util.lang.Validations;

public enum TapePartitionFailureType
{
    AUTO_QUIESCED( Severity.ALERT ),
    CLEANING_TAPE_REQUIRED( Severity.ALERT ),
    DUPLICATE_TAPE_BAR_CODES_DETECTED( Severity.WARNING ),
    EJECT_STALLED_DUE_TO_OFFLINE_TAPES( Severity.ALERT ),
    MINIMUM_DRIVE_COUNT_NOT_MET( Severity.WARNING ),
    MOVE_FAILED( Severity.WARNING ),
    MOVE_FAILED_DUE_TO_PREPARE_TAPE_FOR_REMOVAL_FAILURE( Severity.WARNING ),
    NO_USABLE_DRIVES( Severity.ALERT ),
    ONLINE_STALLED_DUE_TO_NO_STORAGE_SLOTS( Severity.WARNING ),
    TAPE_DRIVE_IN_ERROR( Severity.WARNING ),
    TAPE_DRIVE_MISSING( Severity.WARNING ),
    TAPE_DRIVE_NOT_CLEANED( Severity.WARNING ),
    TAPE_DRIVE_QUIESCED( Severity.INFO ),
    TAPE_DRIVE_TYPE_MISMATCH( Severity.WARNING ),
    TAPE_EJECTION_BY_OPERATOR_REQUIRED( Severity.ALERT ),
    TAPE_MEDIA_TYPE_INCOMPATIBLE( Severity.WARNING ),
    TAPE_REMOVAL_UNEXPECTED( Severity.ALERT ),
    TAPE_IN_INVALID_PARTITION( Severity.ALERT )
    ;
    
    
    private TapePartitionFailureType( final Severity severity )
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
