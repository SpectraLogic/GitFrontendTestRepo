/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsToVerify;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

import java.util.UUID;

public final class TapeTaskUtils
{
    private TapeTaskUtils()
    {
        // singleton
    }
    
    
    public enum FailureHandling
    {
        LOG_IT,
        THROW_EXCEPTION
    }
    
    
    public enum RestoreExpected
    {
        YES,
        NO
    }
    
    
    public static String inspect(
            final Tape tape,
            final UUID tapeDriveId,
            final TapeDriveResource tdResource,
            final TapeFailureManagement tapeFailureManagement)
    {
        Validations.verifyNotNull( "Tape", tape );
        Validations.verifyNotNull( "Tape drive resource", tdResource );
        Validations.verifyNotNull( "Tape failure service", tapeFailureManagement );

        try
        {
            final String result = tdResource.inspect().get( Timeout.VERY_LONG );
            tapeFailureManagement.resetFailures(
                    tape.getId(),
                    tapeDriveId,
                    TapeFailureType.INSPECT_FAILED);
            return result;
        }
        catch ( final RpcException ex )
        {
            TapeFailureType failureType = (tape.isWriteProtected() && tape.getLastCheckpoint() != null)
                    ? TapeFailureType.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY
                    : (ex.getMessage().contains("Medium contains only one partition")
                    ? TapeFailureType.SINGLE_PARTITION
                    : TapeFailureType.INSPECT_FAILED);
            LOG.warn( "Failed to inspect tape.", ex );
            tapeFailureManagement.registerFailure(
                    tape.getId(),
                    failureType,
                    ex );
            throw ex;
        }
    }
    
    
    public static void verifyQuiescedToCheckpoint(
            final Tape tape,
            final TapeDriveResource tdResource,
            final BeansServiceManager serviceManager,
            final TapeFailureManagement tapeFailureManagement,
            final RestoreExpected recordTapeFailure,
            final FailureHandling failureHandling)
    {
        new VerifyQuiescedToCheckpoint( 
                tape, tdResource, serviceManager, tapeFailureManagement, recordTapeFailure, failureHandling );
    }
    
    
    public static S3ObjectsToVerify buildVerifyObjectsPayload( final S3ObjectsOnMedia oom )
    {
        final S3ObjectsToVerify retval = BeanFactory.newBean( S3ObjectsToVerify.class );
        retval.setBuckets( oom.getBuckets() );
        retval.setOptionalS3ObjectMetadataKeys( VERIFY_OPTIONAL_S3_OBJECT_METADATA_KEYS );
        return retval;
    }
    
    
    private final static String [] VERIFY_OPTIONAL_S3_OBJECT_METADATA_KEYS = new String [] { 
        /**
         * If there are multiple blobs per object such that at least some of the blobs were written prior to 
         * the object being completely uploaded to cache, then the creation date will not have been written
         * to tape for some of its blobs written out.  The creation date will be persisted in at least one
         * place, but it's not guaranteed that it will be persisted in every place.
         */
        KeyValueObservable.CREATION_DATE,

        /**
         * If there are multiple blobs per object such that at least some of the blobs were written prior to 
         * the object being completely uploaded to cache, then the etag will not have been written
         * to tape for some of its blobs written out.  The etag will be persisted in at least one
         * place, but it's not guaranteed that it will be persisted in every place.
         */
        S3HeaderType.ETAG.getHttpHeaderName(),
        
        /**
         * Legacy r1.x tapes don't persist the total blob count, and thus, we cannot rely on this being
         * present as a general rule.
         */
        KeyValueObservable.TOTAL_BLOB_COUNT };
    
    final static Logger LOG = Logger.getLogger( TapeTaskUtils.class );
}
