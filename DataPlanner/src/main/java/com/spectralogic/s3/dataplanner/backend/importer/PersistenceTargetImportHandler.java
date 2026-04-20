/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.importer;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;

public interface PersistenceTargetImportHandler< F > extends ImportHandler< F >
{
    /**
     * @return null if the verification succeeded, or non-null for the required return code to retry or
     * give up
     */
    F verify( final ImportPersistenceTargetDirective< ? > importDirective, final S3ObjectsOnMedia objects );


    /**
     * Finalizes an import, taking the final steps to take ownership of the media imported.
     */
    BlobStoreTaskState finalizeImport( final UUID storageDomainId, final UUID isolatedBucketId );
    
    
    void verifyCompatibleStorageDomain( final UUID storageDomainMemberId );
}