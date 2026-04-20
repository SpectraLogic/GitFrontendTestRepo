/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;

public interface BlobDs3TargetService extends BlobTargetService< BlobDs3Target, Ds3DataReplicationRule >
{
    void migrate( final UUID storageDomainId, final Set< BlobDs3Target > blobTargets );
}
