/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.importer;

import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;

public interface ImportHandler< F >
{
    void setImporter( final BaseImporter< ?, ?, F, ? > importer );
    
    
    /**
     * Handle the specified failure, returning the task state the task should return with to either retry
     * or give up
     */
    BlobStoreTaskState failed( 
            final F failureType,
            final RuntimeException ex );
    
    
    /**
     * Warn of a possible issue that the user should be aware of without failing the import. Implementations
     * should create a failure of type F without failing the import.
     */
    void warn( 
            final F failureType,
            final RuntimeException ex );


    /**
     * Must be called before any calls to {@link #read}.
     */
    void openForRead();


    /**
     * @return null if there is nothing else left to read, or non-null for the next chunk of contents read
     */
    S3ObjectsOnMedia read();


    /**
     * Must be called after a call to {@link #openForRead}.
     */
    void closeRead();
}
