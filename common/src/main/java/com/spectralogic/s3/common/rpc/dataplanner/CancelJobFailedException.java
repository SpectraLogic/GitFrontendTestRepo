/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;

public final class CancelJobFailedException extends FailureTypeObservableException
{
    public CancelJobFailedException( 
            final GenericFailure failure, 
            final String message, 
            final Set< UUID > deletedObjectIds )
    {
        super( failure, message );
        m_deletedObjectIds = new HashSet<>( deletedObjectIds );
    }
    
    
    public Set< UUID > getDeletedObjectIds()
    {
        return m_deletedObjectIds;
    }
    
    
    private final Set< UUID > m_deletedObjectIds;
}
