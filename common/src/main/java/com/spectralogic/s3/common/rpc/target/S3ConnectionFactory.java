/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import com.spectralogic.s3.common.dao.domain.target.S3Target;

public interface S3ConnectionFactory extends PublicCloudConnectionFactory< S3Connection, S3Target >
{
    // empty
}
