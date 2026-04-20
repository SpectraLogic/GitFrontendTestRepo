/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;

public interface PublicCloudConnectionFactory
    < C extends PublicCloudConnection, T extends PublicCloudReplicationTarget< T > >
{
    C connect( final T target );
}
