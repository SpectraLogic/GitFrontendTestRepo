/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.api;

public final class BlobStoreTaskNoLongerValidException extends RuntimeException
{
    public BlobStoreTaskNoLongerValidException( final String message )
    {
        super( message );
    }
    
    
    public BlobStoreTaskNoLongerValidException( final Exception cause )
    {
        super( cause );
    }
}