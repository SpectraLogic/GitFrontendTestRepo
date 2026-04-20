/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.tape.RawImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.RawImportTapeDirectiveService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.LongRunningInterruptableTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class RawImportTapeTask 
    extends BaseImportTapeTask< RawImportTapeDirective >
    implements LongRunningInterruptableTapeTask
{
    public RawImportTapeTask(
            final BlobStoreTaskPriority priority,
            final UUID tapeId,
            final BlobStore blobStore,
            final DiskManager diskManager,
            final TapeFailureManagement tapeFailureManagement,
            final BeansServiceManager serviceManager)
    {
        super( RawImportTapeDirectiveService.class,
               TapeState.LTFS_WITH_FOREIGN_DATA,
               TapeState.RAW_IMPORT_PENDING,
               TapeState.RAW_IMPORT_IN_PROGRESS,
               priority,
               tapeId,
               blobStore,
                false,
                diskManager,
                tapeFailureManagement,
                serviceManager );
    }
    
    
    @Override
    protected String openTapeForRead()
    {
        final RawImportTapeDirective importDirective =
                m_serviceManager.getService( RawImportTapeDirectiveService.class ).attainByEntityToImport(
                        getTapeId() );
        final Bucket bucket = m_serviceManager.getRetriever( Bucket.class ).attain( 
                importDirective.getBucketId() );
        return getDriveResource().openForeignContents( 
                bucket.getName(),
                KeyValueObservable.TOTAL_BLOB_COUNT,
                KeyValueObservable.CREATION_DATE,
                64 * 1024L * 1024 * 1024,
                8 * 1024 ).get( Timeout.VERY_LONG );
    }


    @Override
    protected TapeFailureType verifyOnTape( final S3ObjectsOnMedia objects )
    {
        LOG.warn( "Raw content imported cannot be verified, so will skip verification step." );
        return null;
    }
    
    
    @Override
    protected void verifyStorageDomainLtfsFileNamingMatchesTape( final UUID storageDomainId )
    {
        // nothing to verify for raw import
    }
}
