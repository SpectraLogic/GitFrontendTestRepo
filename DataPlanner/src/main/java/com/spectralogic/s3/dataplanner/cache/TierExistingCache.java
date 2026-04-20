/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.cache;

import java.io.IOException;

public interface TierExistingCache
{
    void moveFilesIntoTierStructure() throws IOException, InterruptedException;
    
    void createTieredCacheStructure();
}
