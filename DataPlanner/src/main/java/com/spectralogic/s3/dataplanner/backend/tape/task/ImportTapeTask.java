/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.tape.ImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.ImportTapeDirectiveService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.LongRunningInterruptableTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.render.BytesRenderer;

public final class ImportTapeTask 
    extends BaseImportTapeTask< ImportTapeDirective >
    implements LongRunningInterruptableTapeTask
{
    public ImportTapeTask(
            final BlobStoreTaskPriority priority,
            final UUID tapeId,
            final BlobStore blobStore,
            final DiskManager diskManager,
            final TapeFailureManagement tapeFailureManagement,
            final BeansServiceManager serviceManager)
    {
        super( ImportTapeDirectiveService.class, 
               TapeState.FOREIGN, 
               TapeState.IMPORT_PENDING, 
               TapeState.IMPORT_IN_PROGRESS,
               priority, 
               tapeId, 
               blobStore,
               true,
                diskManager,
                tapeFailureManagement,
                serviceManager);
    }


    @Override
    protected String openTapeForRead()
    {
        return getDriveResource().openDs3Contents( true, true ).get( Timeout.LONG );
    }


    @Override
    protected TapeFailureType verifyOnTape( final S3ObjectsOnMedia objects )
    {
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final long bytesToRead = getTotalWorkInBytes( objects );
        final Duration duration = new Duration();
        final String dataDescription = 
                getNumberOfBlobs( objects ) + " blobs (" + bytesRenderer.render( bytesToRead ) + ")";
        LOG.info( "Will verify " + dataDescription + "..." );
        
        final BlobIoFailures failures = getDriveResource().verifyData( 
                TapeTaskUtils.buildVerifyObjectsPayload( objects ) ).get( Timeout.VERY_LONG );
        if ( 0 == failures.getFailures().length )
        {
            LOG.info( dataDescription + " verified on tape at " 
                      + bytesRenderer.render( bytesToRead, duration ) + "." );
            return null;
        }
        
        LOG.warn( "Failed to import tape " + getTapeId() + " due to data integrity problem: " 
                  + LogUtil.getShortVersion( failures.toJson() ) );
        return TapeFailureType.IMPORT_FAILED_DUE_TO_DATA_INTEGRITY; 
    }
    
    
    private int getNumberOfBlobs( final S3ObjectsOnMedia request )
    {
        int retval = 0;
        for ( final BucketOnMedia bucket : request.getBuckets() )
        {
            for ( final S3ObjectOnMedia o : bucket.getObjects() )
            {
                retval += o.getBlobs().length;
            }
        }
        
        return retval;
    }
}
