/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.util.Comparator;

import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;

final class TapeDriveExecutionPriorityComparator implements Comparator< TapeDrive >
{
    public int compare( final TapeDrive o1, final TapeDrive o2 )
    {
        if ( ( null == o1.getTapeId() ) != ( null == o2.getTapeId() ) )
        {
            return ( null == o1.getTapeId() ) ? -1 : 1;
        }
        return 0;
    }
}
