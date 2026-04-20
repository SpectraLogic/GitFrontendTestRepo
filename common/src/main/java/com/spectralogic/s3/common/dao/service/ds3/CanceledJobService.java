/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.CanceledJob;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface CanceledJobService 
    extends BeansRetriever< CanceledJob >, BeanCreator< CanceledJob >, BeanDeleter
{
    void deleteAll();
    
    
    void markAsTimedOut( final UUID jobId );
}
