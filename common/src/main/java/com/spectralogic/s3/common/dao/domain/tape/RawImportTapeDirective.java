/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.shared.ImportTapeTargetDirective;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes( @Unique( ImportTapeTargetDirective.TAPE_ID ) )
public interface RawImportTapeDirective
    extends ImportTapeTargetDirective< RawImportTapeDirective >, DatabasePersistable
{
    String BUCKET_ID = "bucketId";
    
    @References( Bucket.class )
    @CascadeDelete
    UUID getBucketId();
    
    RawImportTapeDirective setBucketId( final UUID bucketId );
}
