/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique( SerialNumberObservable.SERIAL_NUMBER )
})
@Indexes( @Index( NameObservable.NAME ) )
public interface TapeLibrary
  extends DatabasePersistable, NameObservable< TapeLibrary >, SerialNumberObservable< TapeLibrary >
{
    String MANAGEMENT_URL = "managementUrl";
    
    String getManagementUrl();
    
    TapeLibrary setManagementUrl( final String value );
    
    
    String getName();
}
