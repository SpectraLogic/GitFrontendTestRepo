/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service.api;

import java.util.Set;
import java.util.UUID;
import com.spectralogic.util.db.query.WhereClause;

public interface BeanDeleter
{
    void delete( final UUID beanId );
    
    
    void delete( final Set< UUID > ids );
    
    
    void delete( final WhereClause whereClause );
}
