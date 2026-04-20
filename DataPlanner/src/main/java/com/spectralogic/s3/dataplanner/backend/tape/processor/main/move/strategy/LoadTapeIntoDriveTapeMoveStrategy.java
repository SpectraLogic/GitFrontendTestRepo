/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy;

import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.util.lang.Validations;

import java.util.List;

public final class LoadTapeIntoDriveTapeMoveStrategy extends BaseTapeMoveStrategy
{
    public LoadTapeIntoDriveTapeMoveStrategy( final TapeDrive drive )
    {
        m_drive = drive;
        Validations.verifyNotNull( "Tape drive", m_drive );
    }


    @Override
    protected int getDest()
    {
        return m_tapeEnvironmentManager.getDriveElementAddress( m_drive.getId() );
    }
    

    @Override
    public void commitMove()
    {
        m_tapeEnvironmentManager.moveTapeToDrive( m_tape.getId(), m_drive );
    }


    @Override
    public void rollbackMove()
    {
        // empty
    }


    public List<TapeDrive> getAssociatedDrives() {
        return List.of( m_drive );
    }
    

    private final TapeDrive m_drive;
}
