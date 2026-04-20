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

import java.util.Collections;
import java.util.List;

public final class SlotToSlotTapeMoveStrategy extends BaseTapeMoveStrategy
{
    public SlotToSlotTapeMoveStrategy( final ElementAddressType destinationElementAddressType )
    {
        m_destinationElementAddressType = destinationElementAddressType;
        Validations.verifyNotNull( "Destination element address type", m_destinationElementAddressType );
    }


    @Override
    protected int getDest()
    {
        try
        {
            m_updatedTapeEnvironment = true;
            return m_tapeEnvironmentManager.moveTapeSlotToSlot(
                    m_tape.getId(), m_destinationElementAddressType );
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
            m_tapeEnvironmentManager.moveTapeSlotToSlot( m_tape.getId(), m_src );
        }
    }


    public List<TapeDrive> getAssociatedDrives() {
        return Collections.emptyList();
    }
    

    private volatile boolean m_updatedTapeEnvironment;
    private final ElementAddressType m_destinationElementAddressType;
}
