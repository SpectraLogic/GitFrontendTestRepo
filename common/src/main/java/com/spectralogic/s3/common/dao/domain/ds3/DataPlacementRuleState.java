/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum DataPlacementRuleState
{
    /**
     * The persistence rule is in a normal, included state.
     */
    NORMAL,
    
    
    /**
     * The persistence rule is being created (data copying is required before the persistence rule is in a
     * normal, fully included state).
     */
    INCLUSION_IN_PROGRESS,
}
