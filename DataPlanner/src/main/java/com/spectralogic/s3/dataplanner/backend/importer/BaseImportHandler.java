/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.importer;

import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;

public abstract class BaseImportHandler< F > implements ImportHandler< F >
{
    final public void setImporter( final BaseImporter< ?, ?, F, ? > importer )
    {
        m_importer = importer;
    }
    
    
    final public BlobStoreTaskState failed(
            final F failureType,
            final RuntimeException ex )
    {
        m_importer.deleteImportDirective();
        return failedInternal( failureType, ex );
    }

    
    protected abstract BlobStoreTaskState failedInternal(
            final F failureType,
            final RuntimeException ex );
    
    
    final public void warn( 
            final F failureType,
            final RuntimeException ex )
    {
        warnInternal( failureType, ex );
    }
    
    
    protected abstract void warnInternal(
            final F failureType,
            final RuntimeException ex );
    
    
    private volatile BaseImporter< ?, ?, F, ? > m_importer;
}
