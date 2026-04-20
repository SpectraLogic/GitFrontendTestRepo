/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class TapeState_Test
{
    @Test
    public void testGetStatesThatDisallowInspectionReturnsSensibly()
    {
        assertTrue(TapeState.NORMAL.isInspectionAllowed(), "Shoulda returned sensibly for which states disallow inspection.");
        assertTrue(TapeState.FOREIGN.isInspectionAllowed(), "Shoulda returned sensibly for which states disallow inspection.");
        assertTrue(TapeState.PENDING_INSPECTION.isInspectionAllowed(), "Shoulda returned sensibly for which states disallow inspection.");
        assertTrue(TapeState.DATA_CHECKPOINT_FAILURE.isInspectionAllowed(), "Shoulda returned sensibly for which states disallow inspection.");
        assertFalse(TapeState.BAD.isInspectionAllowed(), "Shoulda returned sensibly for which states disallow inspection.");
        assertFalse(TapeState.EJECTED.isInspectionAllowed(), "Shoulda returned sensibly for which states disallow inspection.");
        assertFalse(TapeState.EJECT_FROM_EE_PENDING.isInspectionAllowed(), "Shoulda returned sensibly for which states disallow inspection.");
        assertFalse(TapeState.EJECT_TO_EE_IN_PROGRESS.isInspectionAllowed(), "Shoulda returned sensibly for which states disallow inspection.");
    }
    
    
    @Test
    public void testGetStatesThatDisallowTapeLoadIntoDriveReturnsSensibly()
    {
        assertTrue(TapeState.getStatesThatDisallowTapeLoadIntoDrive().contains(
                        TapeState.EJECT_FROM_EE_PENDING ), "Shoulda included states that cannot be loaded into drive.");
        assertTrue(TapeState.getStatesThatDisallowTapeLoadIntoDrive().contains( TapeState.EJECTED ), "Shoulda included states that cannot be loaded into drive.");
        assertFalse(TapeState.getStatesThatDisallowTapeLoadIntoDrive().contains( TapeState.FOREIGN ), "Should notta included states that can be loaded into drive.");
    }
    
    
    @Test
    public void testGetStatesThatAreNotPhysicallyPresentReturnsSensibly()
    {
        assertTrue(TapeState.getStatesThatAreNotPhysicallyPresent().contains( 
                        TapeState.EJECT_FROM_EE_PENDING ), "Shoulda included states that are not physically present.");
        assertTrue(TapeState.getStatesThatAreNotPhysicallyPresent().contains( TapeState.EJECTED ), "Shoulda included states that are not physically present.");
        assertFalse(TapeState.getStatesThatAreNotPhysicallyPresent().contains( TapeState.FOREIGN ), "Should notta included states that are physically present.");
    }
}
