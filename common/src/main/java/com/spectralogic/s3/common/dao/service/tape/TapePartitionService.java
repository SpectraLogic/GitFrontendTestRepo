/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface TapePartitionService 
    extends BeansRetriever< TapePartition >, 
            BeanCreator< TapePartition >,
            BeanUpdater< TapePartition >, 
            BeanDeleter
{
    // empty
}
