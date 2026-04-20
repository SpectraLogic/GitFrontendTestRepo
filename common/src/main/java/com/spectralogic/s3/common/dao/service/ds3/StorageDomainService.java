/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.CapacitySummaryContainer;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface StorageDomainService 
    extends BeansRetriever< StorageDomain >,
            BeanCreator< StorageDomain >, 
            BeanUpdater< StorageDomain >, 
            BeanDeleter
{
    CapacitySummaryContainer getCapacitySummary( 
            final UUID storageDomainId, 
            final WhereClause tapeFilter, 
            final WhereClause poolFilter );
    
    
    CapacitySummaryContainer getCapacitySummary( 
            final UUID bucketId,
            final UUID storageDomainId, 
            final WhereClause tapeFilter,
            final WhereClause poolFilter );
    
    
    CapacitySummaryContainer getCapacitySummary( 
            final WhereClause tapeFilter, 
            final WhereClause poolFilter );
    
    
    UUID selectAppropriateStorageDomainMember(
            final PersistenceTarget< ? > pt,
            final UUID storageDomainId );
    
    
    StorageDomain attain( final String identifier );
    
    
    StorageDomain retrieve( final String identifier );
}
