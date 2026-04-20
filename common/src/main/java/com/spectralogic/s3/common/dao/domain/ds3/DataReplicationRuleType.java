/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum DataReplicationRuleType
{
    /**
     * A copy of all data is replicated with this rule.
     */
    PERMANENT,
    
    /**
     * A copy of already-persisted data is replicated with this rule (but no new data shall be replicated with
     * this rule).
     */
    RETIRED
}
