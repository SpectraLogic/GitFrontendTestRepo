/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A {@link Blob} is a binary large object.  An {@link S3Object} that has any binary data to it will always be
 * composed of one or more {@link Blob}s, which when all combined, make up the entire {@link S3Object}'s 
 * binary datum.  <br><br>
 * 
 * A {@link Blob} is the smallest unit of data that can be managed.  For example, either an entire 
 * {@link Blob} is written to tape or none of it is.  Either the entire {@link Blob} is in cache or it isn't.
 * Either the entire {@link Blob} is valid per its checksum or it is entirely corrupted.  <br><br>
 * 
 * Zero-length {@link S3Object}s and folders (which are always zero-length) will always have a single,
 * zero-length {@link Blob}, or in other words every {@link S3Object} record will have at least one 
 * corresponding {@link Blob} record.
 */
@ConcreteImplementation( BlobImpl.class )
@UniqueIndexes(
{
    @Unique({ Blob.BYTE_OFFSET, Blob.OBJECT_ID })
})
public interface Blob extends DatabasePersistable, ChecksumObservable< Blob >
{
    String OBJECT_ID = "objectId";
    
    @References( S3Object.class )
    UUID getObjectId();
    
    Blob setObjectId( final UUID value );
    
    
    String BYTE_OFFSET = "byteOffset";
    
    @SortBy
    long getByteOffset();
    
    Blob setByteOffset( final long value );
    

    String LENGTH = "length";
    
    long getLength();
    
    Blob setLength( final long value );
}
