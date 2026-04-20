/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

public final class NoOpTapeTask extends BaseTapeTask implements StaticTapeTask
{
    public NoOpTapeTask(
            final String taskDescription,
            final BlobStoreTaskPriority priority,
            final UUID tapeId,
            final TapeFailureManagement tapeFailureManagement,
            final BeansServiceManager serviceManager)
    {
        super( priority, tapeId, tapeFailureManagement, serviceManager );
        m_taskDescription = taskDescription;
        
        Validations.verifyNotNull( "Task description", m_taskDescription );
    }

    
    @Override
    protected BlobStoreTaskState runInternal()
    {
        throw new UnsupportedOperationException( getDescription() + " cannot use a tape drive." );
    }
    
    
    public String getDescription()
    {
        return m_taskDescription + " " + m_defaultTapeId;
    }
    

    private final String m_taskDescription;


    @Override
    public boolean allowMultiplePerTape() {
        return true;
    }
}
