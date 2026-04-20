/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface DataPathBackendService 
    extends BeansRetriever< DataPathBackend >, BeanUpdater< DataPathBackend >
{
    boolean isActivated();
    
    
    void dataPathRestarted();
}
