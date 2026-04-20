/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.planner;

import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface CacheFilesystemService 
    extends BeansRetriever< CacheFilesystem >, BeanUpdater< CacheFilesystem >
{
    // empty
}
