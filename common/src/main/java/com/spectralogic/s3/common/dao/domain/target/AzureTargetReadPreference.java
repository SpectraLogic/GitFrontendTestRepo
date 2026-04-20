/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import java.util.UUID;

import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes( @Unique( 
        { 
            TargetReadPreference.TARGET_ID,
            TargetReadPreference.BUCKET_ID
        } ) )
public interface AzureTargetReadPreference
    extends TargetReadPreference< AzureTargetReadPreference >, DatabasePersistable
{
    @CascadeDelete
    @References( AzureTarget.class )
    UUID getTargetId();
}
