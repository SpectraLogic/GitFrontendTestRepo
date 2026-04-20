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

public final class TapeType_Test
{
    @Test
    public void testDataContainingTapesAreReportedAsSuch()
    {
        assertTrue(
                TapeType.LTO6.canContainData(),
                "Data tape shoulda reported as being able to contain data."
                 );
        assertTrue(
                TapeType.getDataContainingTypes().contains( TapeType.LTO6 ),
                "Data tape shoulda reported as being able to contain data."
                 );
        assertFalse(
                TapeType.LTO_CLEANING_TAPE.canContainData(),
                "Non-data tape shoulda reported as not being able to contain data."
                 );
        assertFalse(
                TapeType.getDataContainingTypes().contains( TapeType.LTO_CLEANING_TAPE ),
                "Non-data tape shoulda reported as not being able to contain data."
                );
    }
}
