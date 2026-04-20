/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique({ S3ObjectProperty.OBJECT_ID, KeyValueObservable.KEY })
})
public interface S3ObjectProperty extends DatabasePersistable, KeyValueObservable< S3ObjectProperty >
{
    String OBJECT_ID = "objectId";
    
    @References( S3Object.class )
    @CascadeDelete
    UUID getObjectId();
    
    S3ObjectProperty setObjectId( final UUID value );
}
