/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;

public interface AzureDataReplicationRule
    extends DatabasePersistable, PublicCloudDataReplicationRule< AzureDataReplicationRule >
{
    @References( AzureTarget.class )
    UUID getTargetId();
}
