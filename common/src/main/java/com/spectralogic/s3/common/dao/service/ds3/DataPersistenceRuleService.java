/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.IomType;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface DataPersistenceRuleService 
    extends BeansRetriever< DataPersistenceRule >, 
            BeanCreator< DataPersistenceRule >, 
            BeanUpdater< DataPersistenceRule >, 
            BeanDeleter
{
    List<DataPersistenceRule> getRulesToWriteTo(final UUID dataPolicyId, final IomType jobRestore );
}
