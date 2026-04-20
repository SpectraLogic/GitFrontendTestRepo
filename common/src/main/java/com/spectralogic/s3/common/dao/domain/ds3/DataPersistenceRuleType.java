/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum DataPersistenceRuleType
{
    /**
     * A copy of all data is persisted with this rule.
     */
    PERMANENT,
    
    /**
     * A copy of some data is persisted with this rule.
     */
    TEMPORARY,
    
    /**
     * A copy of already-persisted data is persisted with this rule (but no new data shall be persisted with 
     * this rule).
     */
    RETIRED
}
