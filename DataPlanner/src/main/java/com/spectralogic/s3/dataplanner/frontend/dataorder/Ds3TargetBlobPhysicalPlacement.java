/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.dataorder;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;

public interface Ds3TargetBlobPhysicalPlacement
{
    /**
     * @return the set of targets that have a non-empty subset of the needed blobs physically persisted and
     * available now
     */
    Set< UUID > getCandidateTargets();
    
    
    /**
     * @return the read preference for the specified target
     */
    TargetReadPreferenceType getReadPreference( final UUID targetId );
    
    
    /**
     * @return the set of blobs that are physically persisted and available now on tape for the specified 
     * target
     */
    Set< UUID > getBlobsOnTape( final UUID targetId );
    
    
    /**
     * @return the set of blobs that are physically persisted and available now on pool for the specified 
     * target
     */
    Set< UUID > getBlobsOnPool( final UUID targetId );
}
