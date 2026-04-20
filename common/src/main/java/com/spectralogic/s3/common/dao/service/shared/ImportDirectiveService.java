/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.shared;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface ImportDirectiveService< T extends ImportDirective< T > & DatabasePersistable > 
    extends BeansRetriever< T >, 
            BeanCreator< T >, 
            BeanUpdater< T >
{
    T attainByEntityToImport( final UUID idOfEntityToImport );
    
    
    void deleteByEntityToImport( final UUID idOfEntityToImport );
    
    
    void deleteAll();
}
