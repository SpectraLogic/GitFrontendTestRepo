/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

public enum Ds3TargetAccessControlReplication
{
    /**
     * Nothing on the source is replicated automatically to the target
     */
    NONE,
    
    
    /**
     * Only user operations (create, modify, or delete operations) are replicated to the target; current user
     * set is replicated to the target immediately once this replication level is configured.
     */
    USERS,
}
