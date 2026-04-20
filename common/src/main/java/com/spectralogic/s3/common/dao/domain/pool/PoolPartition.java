/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.pool;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique( NameObservable.NAME )
})
public interface PoolPartition extends NameObservable< PoolPartition >, DatabasePersistable
{
    String TYPE = "type";
    
    PoolType getType();
    
    PoolPartition setType( final PoolType value );
}
