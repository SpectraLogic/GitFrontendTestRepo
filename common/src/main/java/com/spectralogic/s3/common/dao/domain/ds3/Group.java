/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique( NameObservable.NAME )
})
public interface Group extends DatabasePersistable, NameObservable< Group >
{
    String BUILT_IN = "builtIn";
    
    @DefaultBooleanValue( false )
    boolean isBuiltIn();
    
    Group setBuiltIn( final boolean value );
}
