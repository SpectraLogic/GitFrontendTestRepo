/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockdomain;

import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.ConfigureSqlLogLevels;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.SqlLogLevels;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@ConfigureSqlLogLevels( SqlLogLevels.ALL_OPERATIONS_LOGGED_AT_DEBUG_LEVEL )
@UniqueIndexes( @Unique( County.NAME ) )
public interface County extends DatabasePersistable
{
    String NAME = "name";
    
    @SortBy
    String getName();
    
    County setName( final String name );
    
    
    String POPULATION = "population";
    
    long getPopulation();
    
    County setPopulation( final long population );
}
