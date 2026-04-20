/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface DataPolicyService 
    extends BeansRetriever< DataPolicy >, BeanCreator< DataPolicy >, BeanUpdater< DataPolicy >, BeanDeleter
{
    boolean isReplicated( final UUID dataPolicyId );
    
    
    public boolean hasAnyStorageDomainsUsingObjectNamesOnLtfs( final UUID dataPolicyId );
    
    
    public boolean areStorageDomainsWithObjectNamingAllowed( final DataPolicy dp );
    
}
