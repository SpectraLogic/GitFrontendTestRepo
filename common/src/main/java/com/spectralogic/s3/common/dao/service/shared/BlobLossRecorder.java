/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.shared;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.util.db.lang.DatabasePersistable;

public interface BlobLossRecorder< BT extends DatabasePersistable >
{
    /**
     * @param error - if null, this means blobs were lost under normal operation
     * @param persistenceTargetId - the persistence target (e.g. tape or pool) that has lost the blobs
     * @param blobIds - the blobs that have been lost
     */
    void blobsLost( final String error, final UUID persistenceTargetId, final Set< UUID > blobIds );
    

    /**
     * @param error - Cannot be null
     * @param blobTargets - the suspect persistence targets
     */
    void blobsSuspect( final String error, final Set< BT > blobTargets );
}
