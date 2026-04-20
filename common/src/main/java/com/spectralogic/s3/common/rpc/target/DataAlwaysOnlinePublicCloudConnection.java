/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

/**
 * A {@link PublicCloudConnection} where data is always available to be read at any time, without requiring
 * any kind of staging.
 */
public interface DataAlwaysOnlinePublicCloudConnection extends PublicCloudConnection
{
    // empty
}
