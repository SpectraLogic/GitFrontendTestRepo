/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.util.lang.Validations;

import java.util.List;

public final class UnloadTapeFromDriveTapeMoveStrategy extends BaseTapeMoveStrategy
{
    public UnloadTapeFromDriveTapeMoveStrategy(
            final TapeDrive drive, 
            final ElementAddressType destinationSlotType )
    {
        m_drive = drive;
        m_destinationSlotType = destinationSlotType;
        Validations.verifyNotNull( "Tape drive", m_drive );
        Validations.verifyNotNull( "Destination slot type", m_destinationSlotType );
    }


    @Override
    protected int getDest()
    {
        try
        {
            m_updatedTapeEnvironment = true;
            return m_tapeEnvironmentManager.moveTapeFromDrive( m_drive, m_destinationSlotType );
        }
        catch ( final RuntimeException ex )
        {
            m_updatedTapeEnvironment = false;
            throw ex;
        }
    }
    

    @Override
    public void commitMove()
    {
        // empty
    }


    @Override
    public void rollbackMove()
    {
        if ( m_updatedTapeEnvironment )
        {
            m_tapeEnvironmentManager.moveTapeToDrive( m_tape.getId(), m_drive );
        }
    }


    public List<TapeDrive> getAssociatedDrives() {
        return List.of( m_drive );
    }


    private volatile boolean m_updatedTapeEnvironment;
    private final TapeDrive m_drive;
    private final ElementAddressType m_destinationSlotType;
}
