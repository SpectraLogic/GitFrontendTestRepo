/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.api;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;

public interface TapeEjector
{
    /**
     * Ejects the tape specified from the library (moves the tape to the import/export slots).  <br><br>
     * 
     * If <code>verifyPriorToAutoEject</code> is specified, a verify will be scheduled as well at the priority
     * specified.  Else, no verify will be scheduled to occur prior to the eject.
     */
    void ejectTape(
            final BlobStoreTaskPriority verifyPriorToAutoEject,
            final UUID tapeId, 
            final String ejectLabel,
            final String ejectLocation );
}
